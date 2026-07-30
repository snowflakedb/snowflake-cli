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
"""A generic, reusable multi-step progress/checklist widget.

This module has no knowledge of DCM or any other plugin. It lives under the
dcm plugin only because dcm is currently its sole consumer; if a second
plugin needs it, it should move back to snowflake.cli.api.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    TypeVar,
)

from rich import get_console
from rich.console import Group, RenderableType
from rich.padding import Padding
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    Task,
    TaskID,
    TaskProgressColumn,
    TimeElapsedColumn,
)
from rich.spinner import Spinner
from rich.styled import Styled
from rich.table import Table
from rich.text import Text
from snowflake.cli.api.console.console import cli_console

_T = TypeVar("_T")


@dataclass(frozen=True)
class StepDefinition:
    key: str
    label: str


class StepState(str, Enum):
    _terminal: bool

    def __new__(cls, value: str, terminal: bool) -> "StepState":
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj._terminal = terminal  # noqa: SLF001
        return obj

    PENDING = ("pending", False)
    RUNNING = ("running", False)
    DONE = ("done", True)
    FAILED = ("failed", True)

    def is_terminal(self) -> bool:
        return self._terminal


_LABEL_COL_WIDTH = 10
_BAR_WIDTH = 40
_ACTIVE_COLOR = "bright_blue"
_DONE_SYMBOL = "✓"
_FAILED_SYMBOL = "✗"
_PENDING_SYMBOL = "·"
_RUNNING_SYMBOL = Spinner("dots").frames[0]
_DETAIL_INDENT = 2
_DETAIL_STYLE = "dim"

_LABEL_STYLE_BY_STATE = {
    StepState.DONE: "bold green",
    StepState.FAILED: "bold red",
    StepState.RUNNING: f"bold {_ACTIVE_COLOR}",
    StepState.PENDING: "dim",
}


def _render_detail(detail: RenderableType) -> RenderableType:
    """Places one detail component beneath its step, dimmed and indented."""
    return Padding(
        Styled(detail, _DETAIL_STYLE), (0, 0, 0, _DETAIL_INDENT), expand=False
    )


def _format_elapsed(seconds: float) -> str:
    """How long a step took, as ``1h 12m 34s`` - larger units only once reached."""
    hours, rest = divmod(int(seconds), 3600)
    minutes, secs = divmod(rest, 60)
    parts = [f"{hours}h"] if hours else []
    if hours or minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


class _ElapsedColumn(TimeElapsedColumn):
    def render(self, task: Task) -> Text:
        if task.elapsed is None:
            return Text("")
        return Text(_format_elapsed(task.elapsed), style="dim")


@dataclass
class _TrackedStep:
    task_id: TaskID
    label: str
    index: int
    state: StepState = StepState.PENDING
    total: Optional[float] = None


class _ChecklistProgress(Progress):
    """Renders each task as its own hand-composed row instead of rich's
    default aligned-column table.

    A ``Table``'s implicit inter-column padding would both couple sibling
    rows' widths together and add spacing we don't want; composing each
    row's cells with exact, explicit separators keeps every row's width
    independent and its spacing exact.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._bar = BarColumn(
            bar_width=_BAR_WIDTH,
            complete_style=_ACTIVE_COLOR,
            finished_style=_ACTIVE_COLOR,
        )
        self._percent = TaskProgressColumn(
            text_format="{task.percentage:>3.0f}%", style=_ACTIVE_COLOR
        )
        self._spinner = SpinnerColumn(spinner_name="dots", style=_ACTIVE_COLOR)
        self._elapsed = _ElapsedColumn()

    def elapsed_text(self, task: Task) -> str:
        return self._elapsed.render(task).plain

    def get_renderable(self) -> RenderableType:
        rows: List[RenderableType] = [Text("")]
        for task in self.tasks:
            rows.append(self._render_row(task))
            rows.extend(
                _render_detail(detail) for detail in task.fields.get("details", ())
            )
        return Group(*rows)

    def _render_row(self, task: Task) -> Table:
        state = task.fields.get("state", StepState.PENDING)
        style = _LABEL_STYLE_BY_STATE[state]
        label = f"{task.fields['label']:<{_LABEL_COL_WIDTH}}"
        cells = [Text(label, style=style)]

        if state == StepState.DONE:
            cells.append(Text(_DONE_SYMBOL, style=style))
        elif state == StepState.FAILED:
            cells.append(Text(_FAILED_SYMBOL, style=style))
        elif state == StepState.PENDING:
            cells.append(Text(_PENDING_SYMBOL, style=style))
        else:
            cells.append(self._spinner.render(task))
            if task.total is not None:
                cells.append(" ")
                cells.append(self._bar.render(task))
                cells.append(" ")
                cells.append(self._percent.render(task))

        elapsed = self._elapsed.render(task)
        if elapsed.plain:
            cells.append("  ")
            cells.append(elapsed)

        row = Table.grid(padding=0)
        row.add_row(*cells)
        return row


class MultiStepProgress:
    """A reusable multi-step progress display built on rich Progress.

    Each step renders as a row: label (colored by state), state indicator
    (spinner while running, check when done, cross on failure), progress bar
    and percentage (shown only while running with a known total; spinner
    alone otherwise), and elapsed time. A step may also carry detail lines,
    rendered dim and indented directly beneath its own row.

    The component has no knowledge of what the steps represent; callers add
    steps by an arbitrary key and drive their lifecycle.
    """

    def __init__(self, steps: Iterable[StepDefinition]) -> None:
        self._progress = _ChecklistProgress(console=get_console())
        self._steps: Dict[str, _TrackedStep] = {}
        self._display_open = False
        self._is_tty = False
        for step in steps:
            task_id = self._progress.add_task(
                "",
                label=step.label,
                state=StepState.PENDING,
                total=None,
                start=False,
                details=(),
            )
            self._steps[step.key] = _TrackedStep(
                task_id=task_id, label=step.label, index=len(self._steps) + 1
            )
        self._step_count = len(self._steps)

    def step_state(self, key: str) -> StepState:
        return self._steps[key].state

    @property
    def _prints_step_lines(self) -> bool:
        """Whether the live display cannot repaint, so steps print as lines."""
        return self._display_open and not self._is_tty

    def _print_step_line(self, key: str, verb: str) -> None:
        if not self._prints_step_lines:
            return
        step = self._steps[key]
        cli_console.step(
            f"❯ Step {step.index}/{self._step_count} - {step.label} - {verb}"
        )

    def _print_step_details(self, details: Sequence[RenderableType]) -> None:
        """Prints each detail component once, where the display cannot repaint."""
        if not self._prints_step_lines:
            return
        for detail in details:
            cli_console.renderable(_render_detail(detail))

    def _elapsed_suffix(self, key: str) -> str:
        task_id = self._steps[key].task_id
        task = next(t for t in self._progress.tasks if t.id == task_id)
        elapsed = self._progress.elapsed_text(task)
        return f" ({elapsed})" if elapsed else ""

    def start_step(self, key: str, total: Optional[float] = None) -> None:
        step = self._steps[key]
        already_running = step.state == StepState.RUNNING
        self._progress.start_task(step.task_id)
        step.total = total
        step.state = StepState.RUNNING
        self._progress.update(step.task_id, total=total, state=StepState.RUNNING)
        if not already_running:
            self._print_step_line(key, f"{_RUNNING_SYMBOL} Running...")

    def update(self, key: str, completed: float) -> None:
        self._progress.update(self._steps[key].task_id, completed=completed)

    def set_step_details(self, key: str, details: Sequence[RenderableType]) -> None:
        """Attaches components rendered beneath the step's own row.

        Where the live display cannot repaint, they are rendered once and
        printed instead. Any text a component carries must already be
        sanitized by whoever built it.
        """
        self._progress.update(self._steps[key].task_id, details=tuple(details))
        self._print_step_details(details)

    def complete_step(self, key: str) -> None:
        step = self._steps[key]
        if step.total is None:
            self._progress.update(
                step.task_id, total=1, completed=1, state=StepState.DONE
            )
        else:
            self._progress.update(
                step.task_id, completed=step.total, state=StepState.DONE
            )
        step.state = StepState.DONE
        self._progress.stop_task(step.task_id)
        self._print_step_line(
            key, f"{_DONE_SYMBOL} Completed{self._elapsed_suffix(key)}"
        )

    def fail_step(self, key: str) -> None:
        step = self._steps[key]
        self._progress.update(step.task_id, state=StepState.FAILED)
        step.state = StepState.FAILED
        self._progress.stop_task(step.task_id)
        self._print_step_line(
            key, f"{_FAILED_SYMBOL} Failed{self._elapsed_suffix(key)}"
        )

    def run_step(self, key: str, fn: Callable[[StepProgressUpdater], _T]) -> _T:
        self.start_step(key)
        try:
            result = fn(self.step_progress_updater(key))
        except Exception:
            self.fail_step(key)
            raise
        else:
            self.complete_step(key)
        return result

    def fail_running(self) -> None:
        for key, step in self._steps.items():
            if step.state == StepState.RUNNING:
                self.fail_step(key)
                return

    def refresh(self) -> None:
        self._progress.refresh()

    def get_renderable(self) -> RenderableType:
        return self._progress.get_renderable()

    def step_progress_updater(self, key: str) -> StepProgressUpdater:
        return StepProgressUpdater(self, key)

    @contextmanager
    def display(self) -> Iterator[None]:
        self._display_open = True
        self._is_tty = self._progress.console.is_terminal
        if self._is_tty:
            self._progress.start()
        try:
            yield
        finally:
            if self._is_tty:
                self._progress.stop()
            cli_console.message("")
            self._display_open = False


class StepProgressUpdater:
    """A handle to one step of a :class:`MultiStepProgress`.

    Lets a caller drive a single step's lifecycle without any awareness of the
    other steps in the display.
    """

    def __init__(self, progress: MultiStepProgress, key: str) -> None:
        self._progress = progress
        self._key = key

    def start(self, total: Optional[float] = None) -> None:
        self._progress.start_step(self._key, total=total)

    def update(self, completed: float) -> None:
        self._progress.update(self._key, completed=completed)

    def set_details(self, details: Sequence[RenderableType]) -> None:
        self._progress.set_step_details(self._key, details)

    def complete(self) -> None:
        self._progress.complete_step(self._key)

    def fail(self) -> None:
        self._progress.fail_step(self._key)


@contextmanager
def progress_session(progress: MultiStepProgress) -> Iterator[None]:
    """Open the live display for the duration of an operation.

    Silent mode (including machine-readable output formats, which mute
    intermediate output) skips the display entirely; step mutations still
    happen but render nothing, and the operation's work runs unchanged. On
    exception the in-flight step is marked failed before the error propagates.
    """
    if cli_console.is_silent:
        try:
            yield
        except Exception:
            progress.fail_running()
            raise
        return

    with progress.display():
        try:
            yield
        except Exception:
            progress.fail_running()
            progress.refresh()
            raise
