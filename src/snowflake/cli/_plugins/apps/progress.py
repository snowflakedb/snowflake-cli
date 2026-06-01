# Copyright (c) 2026 Snowflake Inc.
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
"""Live phase-checklist progress display for Snowflake App Runtime deploy operations."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Iterator, List, Optional

from rich import get_console
from rich.live import Live
from rich.style import Style
from rich.text import Text
from snowflake.cli.api.cli_global_context import get_cli_context

log = logging.getLogger(__name__)

UPLOAD_PHASE = "UPLOAD"
BUILD_PHASE = "BUILD"
DEPLOY_PHASE = "DEPLOY"

_LIVE_REFRESH_PER_SECOND = 10
_PHASE_COL_WIDTH = 10
_BAR_WIDTH = 40
_BAR_CELL = "━"
_BAR_LEADING_EDGE = "╺"
_SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
_MAX_DETAIL_LINES = 4  # Build log lines / deploy status lines shown below a phase

_PHASE_DONE_STYLE = Style(color="green", bold=True)
_PHASE_RUNNING_STYLE = Style(color="blue", bold=True)
_PHASE_FAILED_STYLE = Style(color="red", bold=True)


def _spinner_glyph() -> str:
    """Pick the current braille spinner glyph from wall-clock time.

    Rich's :class:`Live` re-renders the tracker on every refresh tick;
    deriving the frame from ``time.monotonic()`` turns that into a smooth
    animation without needing a frame counter.
    """
    return _SPINNER_FRAMES[
        int(time.monotonic() * _LIVE_REFRESH_PER_SECOND) % len(_SPINNER_FRAMES)
    ]


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    if secs < 0.05:
        return f"{minutes}m"
    return f"{minutes}m {secs:.0f}s"


class PhaseStatus(str, Enum):
    """Lifecycle state of a single phase in the live checklist."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


@dataclass
class _Phase:
    name: str
    status: PhaseStatus = PhaseStatus.PENDING
    progress: int = 0  # 0-100; only meaningful for UPLOAD (client-side file counting)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    detail_lines: List[str] = field(default_factory=list)

    def observe_running(self, progress: int, ts: datetime) -> None:
        if self.status == PhaseStatus.PENDING:
            self.started_at = ts
        self.status = PhaseStatus.RUNNING
        self.progress = progress

    def observe_done(self, ts: datetime) -> None:
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


class AppDeployProgressTracker:
    """Live phase-checklist progress display for ``snow app deploy``.

    Phases: UPLOAD (client-side) → BUILD → DEPLOY.  Each phase transitions
    through PENDING → RUNNING → DONE (or FAILED).

    * **UPLOAD** shows a pip-style ``━`` progress bar derived from file count.
    * **BUILD** shows a braille spinner and streams the last few build log
      lines as dim sub-details beneath the phase line.
    * **DEPLOY** shows a braille spinner and the latest service status string
      (e.g. ``"url not yet available"``, ``"upgrading"``) as a dim sub-detail.

    Usage::

        tracker = AppDeployProgressTracker(
            run_upload=True, run_build=True, run_deploy=True
        )
        with tracker.session():
            tracker.set_upload_file_total(n)
            for _ in upload_iter:
                tracker.advance_upload()
            tracker.complete_upload()

            tracker.start_build()
            # _poll_until(..., on_status=tracker.update_build_status,
            #             on_poll=log_streamer(on_line=tracker.add_build_log))
            tracker.complete_build()

            tracker.start_deploy()
            # _poll_until(..., on_status=tracker.update_deploy_status)
            tracker.complete_deploy()
    """

    def __init__(
        self,
        *,
        run_upload: bool = True,
        run_build: bool = True,
        run_deploy: bool = True,
    ) -> None:
        phase_names: List[str] = []
        if run_upload:
            phase_names.append(UPLOAD_PHASE)
        if run_build:
            phase_names.append(BUILD_PHASE)
        if run_deploy:
            phase_names.append(DEPLOY_PHASE)
        self._phases: List[_Phase] = [_Phase(name=n) for n in phase_names]
        self._upload_file_total: int = 0
        self._upload_files_done: int = 0
        self._upload_context: str = ""
        self._live: Optional[Live] = None

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _get_phase(self, name: str) -> Optional[_Phase]:
        for phase in self._phases:
            if phase.name == name:
                return phase
        return None

    def _refresh(self) -> None:
        """Force an immediate Live repaint (state changed; don't wait for tick)."""
        if self._live is not None:
            self._live.refresh()

    def _mark_running_phase_failed(self) -> None:
        """On exception, fail the currently-running phase (or first pending one)."""
        ts = datetime.now()
        for phase in self._phases:
            if phase.status == PhaseStatus.RUNNING:
                phase.observe_failed(ts)
                return
        for phase in self._phases:
            if phase.status == PhaseStatus.PENDING:
                phase.observe_failed(ts)
                return

    # ------------------------------------------------------------------ #
    # Upload phase (client-side)                                           #
    # ------------------------------------------------------------------ #

    def set_upload_file_total(self, total: int) -> None:
        """Set expected file count and transition UPLOAD to RUNNING."""
        phase = self._get_phase(UPLOAD_PHASE)
        if phase is None:
            return
        self._upload_file_total = max(0, total)
        self._upload_files_done = 0
        phase.observe_running(0, datetime.now())
        self._refresh()

    def set_upload_context(self, message: str) -> None:
        """Set a dim sub-detail (e.g. stage name) shown below the UPLOAD line."""
        self._upload_context = message
        self._refresh()

    def _upload_percent(self) -> int:
        if self._upload_file_total <= 0:
            return 0
        return min(100, int(100 * self._upload_files_done / self._upload_file_total))

    def advance_upload(self, count: int = 1) -> None:
        """Increment upload progress; repaints only when the displayed % changes."""
        phase = self._get_phase(UPLOAD_PHASE)
        if phase is None or self._upload_file_total <= 0:
            return
        prev_pct = self._upload_percent()
        self._upload_files_done = min(
            self._upload_files_done + count, self._upload_file_total
        )
        new_pct = self._upload_percent()
        if new_pct != prev_pct:
            phase.observe_running(new_pct, datetime.now())
            self._refresh()

    def complete_upload(self) -> None:
        phase = self._get_phase(UPLOAD_PHASE)
        if phase is None:
            return
        phase.observe_done(datetime.now())
        self._refresh()

    def fail_upload(self) -> None:
        """Mark UPLOAD as failed only when it has not already terminated."""
        phase = self._get_phase(UPLOAD_PHASE)
        if phase is None or phase.status in (PhaseStatus.DONE, PhaseStatus.FAILED):
            return
        phase.observe_failed()
        self._refresh()

    # ------------------------------------------------------------------ #
    # Build phase                                                          #
    # ------------------------------------------------------------------ #

    def start_build(self) -> None:
        phase = self._get_phase(BUILD_PHASE)
        if phase is None:
            return
        phase.observe_running(0, datetime.now())
        self._refresh()

    def add_build_log(self, line: str) -> None:
        """Append a build log line; only the last ``_MAX_DETAIL_LINES`` are shown."""
        phase = self._get_phase(BUILD_PHASE)
        if phase is None:
            return
        stripped = line.rstrip()
        if not stripped:
            return
        phase.detail_lines.append(stripped)
        if len(phase.detail_lines) > _MAX_DETAIL_LINES:
            phase.detail_lines = phase.detail_lines[-_MAX_DETAIL_LINES:]
        self._refresh()

    def update_build_status(self, status: str) -> None:
        """No-op: BUILD status (PENDING/RUNNING) is conveyed by the spinner."""

    def complete_build(self) -> None:
        phase = self._get_phase(BUILD_PHASE)
        if phase is None:
            return
        phase.observe_done(datetime.now())
        self._refresh()

    def fail_build(self) -> None:
        phase = self._get_phase(BUILD_PHASE)
        if phase is None or phase.status in (PhaseStatus.DONE, PhaseStatus.FAILED):
            return
        phase.observe_failed()
        self._refresh()

    # ------------------------------------------------------------------ #
    # Deploy phase                                                         #
    # ------------------------------------------------------------------ #

    def start_deploy(self) -> None:
        phase = self._get_phase(DEPLOY_PHASE)
        if phase is None:
            return
        phase.observe_running(0, datetime.now())
        self._refresh()

    def update_deploy_status(self, status: str) -> None:
        """Replace the dim sub-detail below DEPLOY with the latest status string."""
        phase = self._get_phase(DEPLOY_PHASE)
        if phase is None:
            return
        stripped = status.strip()
        if stripped:
            phase.detail_lines = [stripped]
        self._refresh()

    def complete_deploy(self) -> None:
        phase = self._get_phase(DEPLOY_PHASE)
        if phase is None:
            return
        phase.detail_lines = []
        phase.observe_done(datetime.now())
        self._refresh()

    def fail_deploy(self) -> None:
        phase = self._get_phase(DEPLOY_PHASE)
        if phase is None or phase.status in (PhaseStatus.DONE, PhaseStatus.FAILED):
            return
        phase.observe_failed()
        self._refresh()

    # ------------------------------------------------------------------ #
    # Session context manager                                              #
    # ------------------------------------------------------------------ #

    @contextmanager
    def session(self) -> Iterator["AppDeployProgressTracker"]:
        """Keep a single Live display open for the entire deploy operation.

        On any exception the currently-running phase is marked failed before
        the display tears down, so the user sees a ``✗`` rather than a
        half-finished spinner.
        """
        if get_cli_context().silent:
            yield self
            return
        console = get_console()
        with Live(
            self, console=console, refresh_per_second=_LIVE_REFRESH_PER_SECOND
        ) as live:
            self._live = live
            try:
                yield self
            except Exception:
                self._mark_running_phase_failed()
                live.refresh()
                raise
            finally:
                self._live = None

    # ------------------------------------------------------------------ #
    # Rendering (called by Rich on every Live tick via __rich__)           #
    # ------------------------------------------------------------------ #

    def __rich__(self) -> Text:
        """Rich protocol: called on every Live refresh tick.

        Implementing ``__rich__`` lets us pass ``self`` directly to
        :class:`rich.live.Live`, so the spinner animation comes for free —
        no manual repaint is needed for in-place phase updates.
        """
        return self._render()

    def _duration_suffix(self, phase: _Phase) -> str:
        now = datetime.now() if phase.status == PhaseStatus.RUNNING else None
        seconds = phase.duration_seconds(now=now)
        if seconds is None:
            return ""
        return f"  ({_format_duration(seconds)})"

    def _append_progress_bar(self, out: Text, progress: int) -> None:
        """Render a pip-style minimal progress bar.

        Heavy-horizontal cells (``━``) in blue for the completed portion, a
        ``╺`` leading-edge cell at the boundary while in progress, and dim
        ``━`` cells for the remaining portion — followed by ``NNN%`` in blue.
        Matches the aesthetic of pip / Rich's own ``BarColumn``.
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

    def _render_phase_line(self, out: Text, phase: _Phase) -> None:
        duration_str = self._duration_suffix(phase)
        name_col = f"{phase.name:<{_PHASE_COL_WIDTH}}"

        if phase.status == PhaseStatus.DONE:
            out.append(name_col, style=_PHASE_DONE_STYLE)
            out.append("✓", style="bold green")
            out.append(duration_str + "\n", style="dim")
        elif phase.status == PhaseStatus.FAILED:
            out.append(name_col, style=_PHASE_FAILED_STYLE)
            out.append("✗", style="bold red")
            out.append(duration_str + "\n", style="dim")
        elif phase.status == PhaseStatus.RUNNING:
            out.append(name_col, style=_PHASE_RUNNING_STYLE)
            if phase.name == UPLOAD_PHASE and self._upload_file_total > 0:
                self._append_progress_bar(out, phase.progress)
            else:
                # Braille spinner; cycles automatically via Live ticks.
                out.append(_spinner_glyph(), style="blue")
            out.append(duration_str + "\n", style="dim")
        else:  # PENDING
            out.append(name_col + "·\n", style="dim")

    def _render(self) -> Text:
        out = Text("\n")
        for phase in self._phases:
            self._render_phase_line(out, phase)
            # Dim sub-details shown indented below the phase line
            if phase.status in (PhaseStatus.RUNNING, PhaseStatus.DONE):
                if phase.name == UPLOAD_PHASE and self._upload_context:
                    out.append(f"  {self._upload_context}\n", style="dim")
                elif phase.name in (BUILD_PHASE, DEPLOY_PHASE) and phase.detail_lines:
                    for detail in phase.detail_lines:
                        out.append(f"  {detail}\n", style="dim")
        return out
