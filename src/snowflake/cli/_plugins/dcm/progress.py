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

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Callable, Iterator, List, Literal, Optional, Sequence

from rich import get_console
from rich.live import Live
from rich.text import Text
from snowflake.cli._plugins.dcm import styles
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import QueryStatus
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)

UPLOAD_PHASE = "UPLOAD"
ANALYZE_PHASE = "ANALYZE"
BACKEND_PHASES = ["RENDER", "COMPILE", "PLAN", "DEPLOY"]
PLAN_OPERATION_PHASES = ["RENDER", "COMPILE", "PLAN"]
# "analyze" has no server-side progress phases — UPLOAD is rendered, then a
# single ANALYZE phase shows the live spinner while the server analyzes.
OperationMode = Literal["deploy", "plan", "analyze"]

# Server-side phase list per operation mode (UPLOAD is always first; the
# remainder reflects what each command actually runs on the server).
_PHASES_BY_OPERATION: dict[OperationMode, list[str]] = {
    "deploy": [UPLOAD_PHASE, *BACKEND_PHASES],
    "plan": [UPLOAD_PHASE, *PLAN_OPERATION_PHASES],
    "analyze": [UPLOAD_PHASE, ANALYZE_PHASE],
}

# Only PLAN and DEPLOY emit a meaningful 0-100 progress value.
PHASES_WITH_PROGRESS_BAR = {"PLAN", "DEPLOY"}

_FAST_POLL_INTERVAL = 1.0  # seconds — used for the first 100 s
_SLOW_POLL_INTERVAL = 10.0  # seconds — used after 100 s (~85 % fewer calls)
_FAST_POLL_THRESHOLD = 100.0  # seconds

_PHASE_COL_WIDTH = 10
_BAR_WIDTH = 40
_LIVE_REFRESH_PER_SECOND = 10  # ~100 ms per repaint; smooth spinner cadence.

# Heavy-horizontal box-drawing chars for a minimal pip/rich-style progress
# bar: ``━`` cells for both the filled and empty portions (colored vs dim
# to distinguish them) and a ``╺`` "leading edge" cell at the boundary
# while in progress.
_BAR_CELL = "━"
_BAR_LEADING_EDGE = "╺"

# Braille-dot spinner frames; one frame per 100 ms gives a full cycle each
# second when paired with ``_LIVE_REFRESH_PER_SECOND``.
_SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


def _spinner_glyph() -> str:
    """Pick the current braille spinner glyph from wall-clock time.

    Rich's :class:`Live` re-renders the tracker on every refresh tick;
    deriving the frame from ``time.monotonic()`` is what turns that into a
    smooth animation without us needing to bookkeep a frame counter.
    """
    return _SPINNER_FRAMES[
        int(time.monotonic() * _LIVE_REFRESH_PER_SECOND) % len(_SPINNER_FRAMES)
    ]


class PhaseStatus(str, Enum):
    """Lifecycle state of a single phase in the live checklist."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


_TERMINAL_STATUSES = {
    QueryStatus.FAILED_WITH_ERROR,
    QueryStatus.FAILED_WITH_INCIDENT,
    QueryStatus.ABORTED,
    QueryStatus.ABORTING,
}


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    if secs < 0.05:
        return f"{minutes}m"
    return f"{minutes}m {secs:.0f}s"


@dataclass
class _Phase:
    name: str
    status: PhaseStatus = PhaseStatus.PENDING
    progress: int = 0  # 0-100; only meaningful for PHASES_WITH_PROGRESS_BAR
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    hide_timing: bool = False  # Suppress duration display (used for simulated phases)

    def observe_running(self, progress: int, ts: datetime) -> None:
        if self.status == PhaseStatus.PENDING and not self.hide_timing:
            self.started_at = ts
        self.status = PhaseStatus.RUNNING
        self.progress = progress

    def observe_done(self, ts: datetime) -> None:
        if not self.hide_timing:
            if not self.started_at:
                self.started_at = ts
            self.completed_at = ts
        self.status = PhaseStatus.DONE

    def observe_failed(self, ts: Optional[datetime] = None) -> None:
        end = ts or datetime.now()
        if not self.started_at:
            self.started_at = end
        self.completed_at = end
        self.status = PhaseStatus.FAILED

    def duration_seconds(self, *, now: Optional[datetime] = None) -> Optional[float]:
        if not self.started_at:
            return None
        end = self.completed_at or now
        if end is None:
            return None
        return (end - self.started_at).total_seconds()


class DeployProgressTracker:
    """
    Renders a live phase checklist for DCM upload and deploy operations.

    Phases: UPLOAD (client-side) → RENDER → COMPILE → PLAN → DEPLOY (server-side).

    Usage::

        tracker = DeployProgressTracker(conn=manager.connection)
        with tracker.session():
            stage = manager.sync_local_files(..., progress=tracker)
            sfqid = manager.deploy_async(...)
            result = tracker.run_deploy_poll(sfqid)
    """

    def __init__(
        self,
        conn: SnowflakeConnection,
        *,
        operation: OperationMode = "deploy",
    ) -> None:
        self._conn = conn
        self._sfqid: Optional[str] = None
        self._operation = operation
        phase_names = _PHASES_BY_OPERATION[operation]
        self._phases = [_Phase(name=p) for p in phase_names]
        self._upload_stage_message: str = ""
        self._upload_file_summaries: List[str] = []
        self._upload_file_total = 0
        self._upload_files_done = 0
        self._live: Optional[Live] = None

    def __rich__(self) -> Text:
        """Render the current tracker frame.

        Implementing ``__rich__`` lets us pass ``self`` to :class:`rich.live.Live`
        instead of a static snapshot. Rich's refresh loop then calls this
        method on every tick, so spinner animation comes "for free" — no
        manual repaint is needed for in-place phase updates.
        """
        return self._render()

    def _get_phase(self, name: str) -> _Phase:
        for phase in self._phases:
            if phase.name == name:
                return phase
        raise KeyError(name)

    def _refresh_display(self) -> None:
        """Force an immediate Live repaint (state changed; don't wait for tick)."""
        if self._live is not None:
            self._live.refresh()

    # ------------------------------------------------------------------ #
    # Upload phase (client-side)                                            #
    # ------------------------------------------------------------------ #

    def start_upload(self) -> None:
        self._get_phase(UPLOAD_PHASE).observe_running(0, datetime.now())
        self._refresh_display()

    def set_upload_context(
        self, *, stage_message: str, file_summaries: list[str]
    ) -> None:
        self._upload_stage_message = stage_message
        self._upload_file_summaries = file_summaries
        self._refresh_display()

    def set_upload_file_total(self, total: int) -> None:
        """Set expected file count (from the upload plan, no extra I/O)."""
        self._upload_file_total = max(0, total)
        self._upload_files_done = 0
        if self._upload_file_total > 0:
            self._get_phase(UPLOAD_PHASE).observe_running(0, datetime.now())
        self._refresh_display()

    def _upload_percent(self) -> int:
        if self._upload_file_total <= 0:
            return 0
        return min(100, int(100 * self._upload_files_done / self._upload_file_total))

    def advance_upload(self, count: int = 1) -> None:
        """Increment upload progress; refreshes only when the percent changes."""
        if self._upload_file_total <= 0:
            return
        prev_percent = self._upload_percent()
        self._upload_files_done = min(
            self._upload_files_done + count, self._upload_file_total
        )
        new_percent = self._upload_percent()
        if new_percent != prev_percent:
            self._get_phase(UPLOAD_PHASE).observe_running(new_percent, datetime.now())
            self._refresh_display()

    def complete_upload(self) -> None:
        if self._upload_file_total > 0:
            self._get_phase(UPLOAD_PHASE).observe_running(100, datetime.now())
        self._get_phase(UPLOAD_PHASE).observe_done(datetime.now())
        self._refresh_display()

    def fail_upload(self) -> None:
        """Mark UPLOAD as failed only if it has not already terminated.

        A failure raised by a downstream phase (RENDER/COMPILE/PLAN/DEPLOY)
        bubbles up through :meth:`session`'s exception handler; UPLOAD itself
        was already ``done`` at that point and must not be reset to ``failed``.
        """
        upload = self._get_phase(UPLOAD_PHASE)
        if upload.status in (PhaseStatus.DONE, PhaseStatus.FAILED):
            return
        upload.observe_failed()
        self._refresh_display()

    @contextmanager
    def session(self) -> Iterator["DeployProgressTracker"]:
        """Keeps a single Live display open for upload and deploy polling."""
        if get_cli_context().silent:
            yield self
            return

        console = get_console()
        self.start_upload()
        with Live(
            self, console=console, refresh_per_second=_LIVE_REFRESH_PER_SECOND
        ) as live:
            self._live = live
            try:
                yield self
            except Exception:
                self.fail_upload()
                live.refresh()
                raise
            finally:
                self._live = None

    # ------------------------------------------------------------------ #
    # Query-status helpers                                                  #
    # ------------------------------------------------------------------ #

    def _is_still_running(self) -> bool:
        assert self._sfqid is not None
        status = self._conn.get_query_status(self._sfqid)
        return self._conn.is_still_running(status)

    def _query_failed(self) -> bool:
        assert self._sfqid is not None
        return self._conn.get_query_status(self._sfqid) in _TERMINAL_STATUSES

    # ------------------------------------------------------------------ #
    # Progress polling                                                      #
    # ------------------------------------------------------------------ #

    def _poll_progress(self) -> Optional[dict]:
        assert self._sfqid is not None
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
            if phase.name == UPLOAD_PHASE:
                continue
            if phase.name == current_phase_name:
                found_current = True
                bar_progress = progress if phase.name in PHASES_WITH_PROGRESS_BAR else 0
                phase.observe_running(bar_progress, ts)
            elif not found_current:
                if phase.status not in ("done", "failed"):
                    phase.observe_done(ts)

    # ------------------------------------------------------------------ #
    # Finalisation                                                          #
    # ------------------------------------------------------------------ #

    def _finalize_success(self) -> None:
        ts = datetime.now()
        for phase in self._phases:
            if phase.name == UPLOAD_PHASE:
                continue
            if phase.status in (PhaseStatus.PENDING, PhaseStatus.RUNNING):
                phase.observe_done(ts)

    def _finalize_failure(self) -> None:
        ts = datetime.now()
        for phase in reversed(self._phases):
            if phase.name == UPLOAD_PHASE:
                continue
            if phase.status == PhaseStatus.RUNNING:
                phase.observe_failed(ts)
                return
        for phase in self._phases:
            if phase.name == UPLOAD_PHASE:
                continue
            if phase.status == PhaseStatus.PENDING:
                phase.observe_failed(ts)
                return

    # ------------------------------------------------------------------ #
    # Rendering                                                             #
    # ------------------------------------------------------------------ #

    def _duration_suffix(self, phase: _Phase) -> str:
        if phase.hide_timing:
            return ""
        now = datetime.now() if phase.status == PhaseStatus.RUNNING else None
        seconds = phase.duration_seconds(now=now)
        if seconds is None:
            return ""
        return f"  ({_format_duration(seconds)})"

    def _simulate_instant_phase(self, name: str) -> None:
        phase = self._get_phase(name)
        phase.hide_timing = True
        phase.status = PhaseStatus.DONE

    def _phase_shows_progress_bar(self, phase: _Phase) -> bool:
        if phase.name == UPLOAD_PHASE:
            return self._upload_file_total > 0
        if phase.name in PHASES_WITH_PROGRESS_BAR:
            return self._operation == "deploy"
        return False

    def _append_progress_bar(self, out: Text, progress: int) -> None:
        """Render a pip-style minimal progress bar.

        Heavy-horizontal cells (``━``) in blue for the completed portion, a
        ``╺`` leading-edge cell at the boundary while in progress, and dim
        ``━`` cells for the remaining portion — followed by " NNN%" in
        blue. No surrounding brackets. Matches the look of pip/rich's
        ``BarColumn`` so the UI feels familiar.
        """
        filled = int(_BAR_WIDTH * progress / 100)
        in_progress = 0 < filled < _BAR_WIDTH

        if in_progress:
            out.append(_BAR_CELL * filled + _BAR_LEADING_EDGE, style="blue")
            out.append(_BAR_CELL * (_BAR_WIDTH - filled - 1), style="dim")
        elif filled == 0:
            out.append(_BAR_CELL * _BAR_WIDTH, style="dim")
        else:
            out.append(_BAR_CELL * _BAR_WIDTH, style="blue")

        out.append(f" {progress:>3}%", style="blue")

    def _append_upload_details(self, out: Text) -> None:
        """Render the stage-creation message and folder counters indented
        beneath the UPLOAD phase line (dim, two-space indent)."""
        if self._upload_stage_message:
            out.append(f"  {self._upload_stage_message}\n", style="dim")
        for summary in self._upload_file_summaries:
            out.append(f"  {summary}\n", style="dim")

    def _render_phase_line(self, out: Text, phase: _Phase) -> None:
        # Wall-clock start time is intentionally omitted — the parenthesised
        # elapsed duration that follows the status indicator is the single
        # source of timing information.
        duration_str = self._duration_suffix(phase)
        name_col = f"{phase.name:<{_PHASE_COL_WIDTH}}"

        if phase.status == PhaseStatus.DONE:
            out.append(name_col, style=styles.PHASE_DONE_STYLE)
            out.append("✓", style="bold green")
            out.append(duration_str + "\n", style="dim")
        elif phase.status == PhaseStatus.FAILED:
            out.append(name_col, style=styles.PHASE_FAILED_STYLE)
            out.append("✗", style="bold red")
            out.append(duration_str + "\n", style="dim")
        elif phase.status == PhaseStatus.RUNNING:
            out.append(name_col, style=styles.PHASE_RUNNING_STYLE)
            if self._phase_shows_progress_bar(phase):
                self._append_progress_bar(out, phase.progress)
            else:
                # Animated braille spinner; Rich's Live re-renders us each
                # refresh tick (see :meth:`__rich__`), so the glyph cycles.
                out.append(_spinner_glyph(), style="yellow")
            out.append(duration_str + "\n", style="dim")
        else:  # PENDING
            out.append(name_col + "·\n", style="dim")

    def _render(self) -> Text:
        out = Text("\n")
        for phase in self._phases:
            self._render_phase_line(out, phase)
            if phase.name == UPLOAD_PHASE and (
                self._upload_stage_message or self._upload_file_summaries
            ):
                self._append_upload_details(out)
        return out

    # ------------------------------------------------------------------ #
    # Main entry points                                                     #
    # ------------------------------------------------------------------ #

    @contextmanager
    def _ensure_live(self) -> Iterator[Optional[Live]]:
        """Yield a Live instance to drive repaints, or ``None`` when silent.

        Three cases collapse into one:

        * silent → yield ``None``; callers skip repaints entirely.
        * a session is already active → reuse ``self._live``.
        * standalone call → open a fresh Live around this block.
        """
        if get_cli_context().silent:
            yield None
            return
        if self._live is not None:
            yield self._live
            return
        with Live(
            self,
            console=get_console(),
            refresh_per_second=_LIVE_REFRESH_PER_SECOND,
        ) as live:
            previous = self._live
            self._live = live
            try:
                yield live
            finally:
                self._live = previous

    def run_deploy_poll(self, sfqid: str) -> SnowflakeCursor:
        """Poll server-side deploy progress until the query finishes.

        Call inside :meth:`session` after upload completes.
        """
        self._sfqid = sfqid
        self.complete_upload()

        start = time.monotonic()
        with self._ensure_live() as live:
            while self._is_still_running():
                self._update_from_poll(self._poll_progress())
                elapsed = time.monotonic() - start
                interval = (
                    _FAST_POLL_INTERVAL
                    if elapsed < _FAST_POLL_THRESHOLD
                    else _SLOW_POLL_INTERVAL
                )
                time.sleep(interval)

            if self._query_failed():
                self._finalize_failure()
            else:
                self._finalize_success()
            if live is not None:
                live.refresh()

        result_cursor = self._conn.cursor()
        result_cursor.get_results_from_sfqid(self._sfqid)
        return result_cursor

    def run_loader_phase(
        self,
        execute_fn: Callable[[], SnowflakeCursor],
        *,
        phase_name: str,
        simulated_phases: Sequence[str] = (),
    ) -> SnowflakeCursor:
        """Run a blocking SQL operation while the spinner animates in place.

        The phase named ``phase_name`` is marked running before ``execute_fn``
        is invoked; the braille spinner glyph cycles next to it via Rich's
        Live auto-refresh (see :meth:`__rich__`). On success the phase
        transitions to done with a real duration; on exception it transitions
        to failed.

        ``simulated_phases`` are marked instantly done first — used by ``plan``
        to fast-forward ``RENDER`` / ``COMPILE`` before settling on ``PLAN``.
        """
        self.complete_upload()
        for name in simulated_phases:
            self._simulate_instant_phase(name)

        phase = self._get_phase(phase_name)
        phase.observe_running(0, datetime.now())
        self._refresh_display()

        if get_cli_context().silent:
            return execute_fn()

        try:
            result = execute_fn()
        except Exception:
            phase.observe_failed(datetime.now())
            raise
        else:
            phase.observe_done(datetime.now())
        return result
