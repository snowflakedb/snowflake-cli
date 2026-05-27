# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Live phase-checklist progress display for DCM deploy operations."""

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from rich import get_console
from rich.live import Live
from rich.text import Text
from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import QueryStatus
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)

# Canonical phase order for a deploy operation.
EXPECTED_PHASES = ["RENDER", "COMPILE", "PLAN", "DEPLOY"]

# Only PLAN and DEPLOY emit a meaningful 0-100 progress value.
PHASES_WITH_PROGRESS_BAR = {"PLAN", "DEPLOY"}

_FAST_POLL_INTERVAL = 1.0  # seconds — used for the first 100 s
_SLOW_POLL_INTERVAL = 10.0  # seconds — used after 100 s (~85 % fewer calls)
_FAST_POLL_THRESHOLD = 100.0  # seconds

_PHASE_COL_WIDTH = 10
_BAR_WIDTH = 20

_TERMINAL_STATUSES = {
    QueryStatus.FAILED_WITH_ERROR,
    QueryStatus.FAILED_WITH_INCIDENT,
    QueryStatus.ABORTED,
    QueryStatus.ABORTING,
}


@dataclass
class _Phase:
    name: str
    status: str = "pending"  # pending | running | done | failed
    progress: int = 0  # 0-100; only meaningful for PHASES_WITH_PROGRESS_BAR
    started_at: Optional[datetime] = None

    def observe_running(self, progress: int, ts: datetime) -> None:
        if self.status == "pending":
            self.started_at = ts
        self.status = "running"
        self.progress = progress

    def observe_done(self, ts: datetime) -> None:
        if not self.started_at:
            self.started_at = ts
        self.status = "done"

    def observe_failed(self) -> None:
        if not self.started_at:
            self.started_at = datetime.now()
        self.status = "failed"


class DeployProgressTracker:
    """
    Polls SYSTEM$GET_DCM_PROJECT_PROGRESS for a running deploy query and renders
    a live phase checklist in the terminal.

    Phases: RENDER → COMPILE → PLAN → DEPLOY.
    PLAN and DEPLOY emit a 0-100 progress value; the other phases show a spinner.

    Poll cadence: 1 Hz for the first 100 s, then 0.1 Hz.

    Usage::

        sfqid = manager.deploy_async(...)
        tracker = DeployProgressTracker(conn=manager.connection, sfqid=sfqid)
        result_cursor = tracker.run()   # blocks; renders live; returns cursor
    """

    def __init__(self, conn: SnowflakeConnection, sfqid: str) -> None:
        self._conn = conn
        self._sfqid = sfqid
        self._phases = [_Phase(name=p) for p in EXPECTED_PHASES]

    # ------------------------------------------------------------------ #
    # Query-status helpers                                                  #
    # ------------------------------------------------------------------ #

    def _is_still_running(self) -> bool:
        status = self._conn.get_query_status(self._sfqid)
        return self._conn.is_still_running(status)

    def _query_failed(self) -> bool:
        return self._conn.get_query_status(self._sfqid) in _TERMINAL_STATUSES

    # ------------------------------------------------------------------ #
    # Progress polling                                                      #
    # ------------------------------------------------------------------ #

    def _poll_progress(self) -> Optional[dict]:
        try:
            cursor = self._conn.cursor()
            cursor.execute(f"SELECT SYSTEM$GET_DCM_PROJECT_PROGRESS('{self._sfqid}')")
            row = cursor.fetchone()
            if row and row[0]:
                return json.loads(row[0])
        except Exception:  # noqa: BLE001
            log.debug(
                "Progress poll failed for sfqid=%s; will retry next cycle.",
                self._sfqid,
                exc_info=True,
            )
        return None

    def _update_from_poll(self, data: Optional[dict]) -> None:
        if not data:
            return
        current_phase_name = data.get("phase", "").upper()
        progress = int(data.get("progress", 0))
        ts = datetime.now()

        found_current = False
        for phase in self._phases:
            if phase.name == current_phase_name:
                found_current = True
                bar_progress = progress if phase.name in PHASES_WITH_PROGRESS_BAR else 0
                phase.observe_running(bar_progress, ts)
            elif not found_current:
                # Canonical backfill: this phase finished before our poll cadence caught it.
                if phase.status not in ("done", "failed"):
                    phase.observe_done(ts)
            # Phases after current remain pending.

    # ------------------------------------------------------------------ #
    # Finalisation                                                          #
    # ------------------------------------------------------------------ #

    def _finalize_success(self) -> None:
        ts = datetime.now()
        for phase in self._phases:
            if phase.status in ("pending", "running"):
                phase.observe_done(ts)

    def _finalize_failure(self) -> None:
        # Mark the last running phase as failed; leave pending phases as-is.
        for phase in reversed(self._phases):
            if phase.status == "running":
                phase.observe_failed()
                return
        # If no running phase was ever observed, mark the first pending one.
        for phase in self._phases:
            if phase.status == "pending":
                phase.observe_failed()
                return

    # ------------------------------------------------------------------ #
    # Rendering                                                             #
    # ------------------------------------------------------------------ #

    def _render(self) -> Text:
        out = Text()
        for phase in self._phases:
            ts_str = (
                f"  {phase.started_at.strftime('%H:%M:%S')}" if phase.started_at else ""
            )
            name_col = f"  {phase.name:<{_PHASE_COL_WIDTH}}"

            if phase.status == "done":
                out.append(name_col, style="bold")
                out.append("✓", style="bold green")
                out.append(ts_str + "\n", style="dim")
            elif phase.status == "failed":
                out.append(name_col, style="bold")
                out.append("✗", style="bold red")
                out.append(ts_str + "\n", style="dim")
            elif phase.status == "running":
                out.append(name_col, style="bold")
                if phase.name in PHASES_WITH_PROGRESS_BAR:
                    filled = int(_BAR_WIDTH * phase.progress / 100)
                    bar = "█" * filled + "░" * (_BAR_WIDTH - filled)
                    out.append(f"[{bar}] {phase.progress:>3}%", style="cyan")
                else:
                    out.append("…", style="yellow")
                out.append(ts_str + "\n", style="dim")
            else:  # pending
                out.append(name_col + "·\n", style="dim")
        return out

    # ------------------------------------------------------------------ #
    # Main entry point                                                      #
    # ------------------------------------------------------------------ #

    def run(self) -> SnowflakeCursor:
        """
        Poll progress until the deploy query finishes, rendering a live phase
        checklist.  Returns the result cursor (or raises on SQL failure).
        """
        from snowflake.cli.api.cli_global_context import get_cli_context

        silent = get_cli_context().silent
        start = time.monotonic()

        def _tick() -> None:
            data = self._poll_progress()
            self._update_from_poll(data)

        def _sleep(elapsed: float) -> None:
            interval = (
                _FAST_POLL_INTERVAL
                if elapsed < _FAST_POLL_THRESHOLD
                else _SLOW_POLL_INTERVAL
            )
            time.sleep(interval)

        if silent:
            while self._is_still_running():
                _tick()
                _sleep(time.monotonic() - start)
            if self._query_failed():
                self._finalize_failure()
            else:
                self._finalize_success()
        else:
            console = get_console()
            with Live(self._render(), console=console, refresh_per_second=4) as live:
                while self._is_still_running():
                    _tick()
                    live.update(self._render())
                    _sleep(time.monotonic() - start)

                if self._query_failed():
                    self._finalize_failure()
                else:
                    self._finalize_success()
                live.update(self._render())

        result_cursor = self._conn.cursor()
        result_cursor.get_results_from_sfqid(self._sfqid)
        return result_cursor
