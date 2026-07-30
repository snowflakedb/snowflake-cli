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
"""Rendering tests for the reusable MultiStepProgress component."""

import re
from contextlib import AbstractContextManager
from io import StringIO
from typing import NoReturn
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console
from rich.spinner import Spinner
from rich.text import Text
from snowflake.cli._plugins.dcm import multistep_progress as progress_module
from snowflake.cli._plugins.dcm.multistep_progress import (
    MultiStepProgress,
    StepDefinition,
    StepProgressUpdater,
    StepState,
    progress_session,
)

from tests.dcm.multi_step_progress_capture import ANSI_RE, capture_rendered, find_line

_CONTROL_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
_SPINNER_FRAMES = Spinner("dots").frames
_SILENT = "snowflake.cli.api.console.abc.get_cli_context"


def _truecolor_console(width: int = 90) -> Console:
    return Console(
        file=StringIO(),
        force_terminal=True,
        color_system="truecolor",
        width=width,
        legacy_windows=False,
    )


def _non_terminal_console() -> Console:
    return Console(file=StringIO(), force_terminal=False)


class TestStepRendering:
    def test_pending_step_shows_marker_and_no_bar(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])

        # when
        line = find_line(progress, "STEP_A")

        # then
        assert "·" in line
        assert "━" not in line
        assert "%" not in line

    def test_running_determinate_shows_bar_and_percent(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a", total=100)
        progress.update("a", completed=50)

        # when
        line = find_line(progress, "STEP_A")

        # then
        assert any(frame in line for frame in _SPINNER_FRAMES)
        assert "━" in line
        assert "50%" in line

    def test_running_indeterminate_shows_only_spinner(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a")

        # when
        line = find_line(progress, "STEP_A")

        # then
        assert any(frame in line for frame in _SPINNER_FRAMES)
        assert "━" not in line
        assert "%" not in line

    def test_done_determinate_shows_check_and_hides_bar_and_percent(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a", total=100)
        progress.complete_step("a")

        # when
        line = find_line(progress, "STEP_A")

        # then
        assert "✓" in line
        assert "━" not in line
        assert "%" not in line

    def test_done_indeterminate_shows_check_and_hides_bar_and_percent(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a")
        progress.complete_step("a")

        # when
        line = find_line(progress, "STEP_A")

        # then
        assert "✓" in line
        assert "━" not in line
        assert "%" not in line

    def test_failed_step_shows_cross_and_no_bar(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a")
        progress.fail_step("a")

        # when
        line = find_line(progress, "STEP_A")

        # then
        assert "✗" in line
        assert "━" not in line

    def test_elapsed_time_rendered_for_finished_step(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a", total=100)
        progress.complete_step("a")

        # when
        line = find_line(progress, "STEP_A")

        # then
        assert re.search(r"\b\d+s\b", line)


class TestFormatElapsed:
    @pytest.mark.parametrize(
        "seconds, expected",
        [
            (0, "0s"),
            (5, "5s"),
            (59, "59s"),
            (60, "1m 0s"),
            (72, "1m 12s"),
            (599, "9m 59s"),
            (3600, "1h 0m 0s"),
            (4354, "1h 12m 34s"),
            (90061, "25h 1m 1s"),
            (72.9, "1m 12s"),
        ],
    )
    def test_larger_units_appear_only_once_reached(
        self, seconds: float, expected: str
    ) -> None:
        # given / when / then
        assert progress_module._format_elapsed(seconds) == expected  # noqa: SLF001


class TestStepColors:
    def test_in_progress_bar_is_bright_blue(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a", total=100)
        progress.update("a", completed=50)
        console = _truecolor_console()

        # when
        console.print(progress.get_renderable())

        # then
        raw = console.file.getvalue()
        assert "\x1b[94m━" in raw

    def test_bar_stays_bright_blue_at_full_completion_while_still_running(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a", total=10)
        console = _truecolor_console()

        # when: reaches 100% via update(), before complete_step() is called
        progress.update("a", completed=10)
        console.print(progress.get_renderable())

        # then: the bar must stay bright blue, not rich's default "finished" green
        raw = console.file.getvalue()
        assert "\x1b[94m━" in raw

    def test_elapsed_time_rendered_dim(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a", total=100)
        progress.complete_step("a")
        console = _truecolor_console()

        # when
        console.print(progress.get_renderable())

        # then
        raw = console.file.getvalue()
        assert re.search(r"\x1b\[2m\d+s", raw)

    def test_percentage_rendered_bright_blue(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a", total=100)
        progress.update("a", completed=50)
        console = _truecolor_console()

        # when
        console.print(progress.get_renderable())

        # then
        raw = console.file.getvalue()
        assert re.search(r"\x1b\[94m\s*\d+%", raw)
        assert "50%" in ANSI_RE.sub("", raw)

    def test_label_color_reflects_state(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        console = _truecolor_console()

        def rendered() -> str:
            console.file = StringIO()
            console.print(progress.get_renderable())
            return console.file.getvalue()

        # then: pending is dim
        assert "\x1b[2mSTEP_A" in rendered()

        # when: running
        progress.start_step("a")
        # then: bold bright blue
        assert "\x1b[1;94mSTEP_A" in rendered()

        # when: done
        progress.complete_step("a")
        # then: bold green
        assert "\x1b[1;32mSTEP_A" in rendered()

        # given: a separate step taken to failure
        progress2 = MultiStepProgress([StepDefinition("b", "STEP_B")])
        progress2.start_step("b")
        progress2.fail_step("b")
        console2 = _truecolor_console()

        # when
        console2.print(progress2.get_renderable())

        # then: bold red
        assert "\x1b[1;31mSTEP_B" in console2.file.getvalue()


class TestMultiStepRendering:
    def test_multiple_steps_render_in_insertion_order(self) -> None:
        # given
        progress = MultiStepProgress(
            [
                StepDefinition("a", "FIRST"),
                StepDefinition("b", "SECOND"),
                StepDefinition("c", "THIRD"),
            ]
        )

        # when
        rendered = capture_rendered(progress)
        lines = [line for line in rendered.split("\n") if line.strip()]

        # then
        assert "FIRST" in lines[0]
        assert "SECOND" in lines[1]
        assert "THIRD" in lines[2]

    def test_leading_blank_line_is_part_of_rendered_region(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])

        # when
        rendered = capture_rendered(progress)

        # then
        assert rendered.split("\n")[0] == ""
        assert "STEP_A" in rendered.split("\n")[1]

    def test_blank_line_rendered_before_and_after_component(self, capsys) -> None:
        # given
        console = _truecolor_console(width=60)
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "STEP_A")])

            # when
            with progress.display():
                progress.run_step("a", lambda step: None)

        rendered = _CONTROL_RE.sub("", console.file.getvalue())

        # then
        assert rendered.split("\n")[0] == ""
        assert capsys.readouterr().out == "\n"


class TestStepLifecycle:
    def test_states_progress_through_lifecycle(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        assert progress.step_state("a") == StepState.PENDING

        # when
        progress.start_step("a")
        # then
        assert progress.step_state("a") == StepState.RUNNING

        # when
        progress.complete_step("a")
        # then
        assert progress.step_state("a") == StepState.DONE

    def test_fail_sets_failed_state(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a")

        # when
        progress.fail_step("a")

        # then
        assert progress.step_state("a") == StepState.FAILED

    def test_handle_drives_only_its_own_step(self) -> None:
        # given
        progress = MultiStepProgress(
            [StepDefinition("a", "STEP_A"), StepDefinition("b", "STEP_B")]
        )

        # when
        progress.step_progress_updater("a").start()
        progress.step_progress_updater("a").complete()

        # then
        assert progress.step_state("a") == StepState.DONE
        assert progress.step_state("b") == StepState.PENDING

    def test_handle_fail_marks_step_failed(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        updater = progress.step_progress_updater("a")
        updater.start()

        # when
        updater.fail()

        # then
        assert progress.step_state("a") == StepState.FAILED


class TestRefresh:
    def test_refresh_delegates_to_underlying_progress(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress._progress = MagicMock()  # noqa: SLF001

        # when
        progress.refresh()

        # then
        progress._progress.refresh.assert_called_once()  # noqa: SLF001


class TestRunStep:
    def test_runs_fn_and_completes_step(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])

        # when
        result = progress.run_step("a", lambda step: "value")

        # then
        assert result == "value"
        assert progress.step_state("a") == StepState.DONE

    def test_passes_step_handle_to_fn(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])

        # when
        received = progress.run_step("a", lambda step: step)

        # then
        assert isinstance(received, StepProgressUpdater)

    def test_noop_step_starts_and_completes(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])

        # when
        result = progress.run_step("a", lambda step: None)

        # then
        assert result is None
        assert progress.step_state("a") == StepState.DONE

    def test_failure_marks_step_failed_and_reraises(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])

        class _BoomError(RuntimeError):
            pass

        def _raise(step: StepProgressUpdater) -> NoReturn:
            raise _BoomError("boom")

        # when
        try:
            progress.run_step("a", _raise)
        except _BoomError:
            pass
        else:
            raise AssertionError("Expected _BoomError")

        # then
        assert progress.step_state("a") == StepState.FAILED


class TestFailRunning:
    def test_marks_running_step_failed(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a")

        # when
        progress.fail_running()

        # then
        assert progress.step_state("a") == StepState.FAILED

    def test_does_not_reset_a_done_step(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])
        progress.start_step("a")
        progress.complete_step("a")

        # when
        progress.fail_running()

        # then
        assert progress.step_state("a") == StepState.DONE

    def test_noop_when_nothing_running(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "STEP_A")])

        # when
        progress.fail_running()

        # then
        assert progress.step_state("a") == StepState.PENDING


class TestStepStateIsTerminal:
    def test_done_and_failed_are_terminal(self) -> None:
        # given / when / then
        assert StepState.DONE.is_terminal()
        assert StepState.FAILED.is_terminal()

    def test_pending_and_running_are_not_terminal(self) -> None:
        # given / when / then
        assert not StepState.PENDING.is_terminal()
        assert not StepState.RUNNING.is_terminal()


class TestNonTtyStepLines:
    def test_start_step_prints_running_line(self, capsys) -> None:
        # given
        console = _non_terminal_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])
            with progress.display():
                # when
                progress.start_step("a")

        # then
        assert (
            f"❯ Step 1/1 - UPLOAD - {_SPINNER_FRAMES[0]} Running...\n"
            in capsys.readouterr().out
        )

    def test_complete_step_prints_completed_line(self, capsys) -> None:
        # given
        console = _non_terminal_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])
            with progress.display():
                progress.start_step("a")
                # when
                progress.complete_step("a")

        # then
        assert re.search(
            r"❯ Step 1/1 - UPLOAD - ✓ Completed \(\d+s\)\n",
            capsys.readouterr().out,
        )

    def test_fail_step_prints_failed_line(self, capsys) -> None:
        # given
        console = _non_terminal_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])
            with progress.display():
                progress.start_step("a")
                # when
                progress.fail_step("a")

        # then
        assert re.search(
            r"❯ Step 1/1 - UPLOAD - ✗ Failed \(\d+s\)\n",
            capsys.readouterr().out,
        )

    def test_repeated_start_step_prints_running_line_only_once(self, capsys) -> None:
        # given: simulates ServerPoll calling start_step on every poll cycle
        console = _non_terminal_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "PLAN")])
            with progress.display():
                # when
                progress.start_step("a", total=100)
                progress.start_step("a", total=100)
                progress.start_step("a", total=100)

        # then
        assert capsys.readouterr().out.count("Running...") == 1

    def test_step_numbering_reflects_position_and_total(self, capsys) -> None:
        # given
        console = _non_terminal_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress(
                [
                    StepDefinition("a", "UPLOAD"),
                    StepDefinition("b", "RENDER"),
                    StepDefinition("c", "COMPILE"),
                ]
            )
            with progress.display():
                # when
                progress.start_step("b")

        # then
        printed = capsys.readouterr().out
        assert "Step 2/3 - RENDER - " in printed
        assert "Running..." in printed

    def test_no_step_lines_printed_on_terminal_console(self, capsys) -> None:
        # given
        console = _truecolor_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])
            # when
            with progress.display():
                progress.run_step("a", lambda step: None)

        # then
        assert "❯" not in capsys.readouterr().out
        assert "❯" not in console.file.getvalue()

    def test_no_step_lines_printed_outside_display_session(self, capsys) -> None:
        # given: simulates silent mode, where progress.display() is never entered
        console = _non_terminal_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])
            # when
            progress.start_step("a")
            progress.complete_step("a")

        # then
        assert capsys.readouterr().out == ""
        assert console.file.getvalue() == ""

    def test_run_step_noop_prints_running_then_completed(self, capsys) -> None:
        # given: mirrors the `plan` command's fast-forwarded RENDER/COMPILE steps
        console = _non_terminal_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "RENDER")])
            with progress.display():
                # when
                progress.run_step("a", lambda step: None)

        # then
        lines = [ln for ln in capsys.readouterr().out.split("\n") if ln]
        assert lines[0] == f"❯ Step 1/1 - RENDER - {_SPINNER_FRAMES[0]} Running..."
        assert re.fullmatch(r"❯ Step 1/1 - RENDER - ✓ Completed \(\d+s\)", lines[1])


class TestStepDetails:
    def test_details_render_directly_beneath_their_own_step(self) -> None:
        # given
        progress = MultiStepProgress(
            [StepDefinition("a", "UPLOAD"), StepDefinition("b", "RENDER")]
        )
        progress.start_step("a")

        # when
        progress.set_step_details("a", [Text("first detail"), Text("second detail")])

        # then
        lines = [line for line in capture_rendered(progress).split("\n") if line]
        assert "UPLOAD" in lines[0]
        assert lines[1] == "  first detail"
        assert lines[2] == "  second detail"
        assert "RENDER" in lines[3]

    def test_details_are_rendered_dim(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])
        console = _truecolor_console()

        # when
        progress.set_step_details("a", [Text("a detail")])
        console.print(progress.get_renderable())

        # then
        assert "  \x1b[2ma detail" in console.file.getvalue()

    def test_details_survive_step_state_changes(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])
        progress.start_step("a", total=2)
        progress.set_step_details("a", [Text("a detail")])

        # when
        progress.update("a", completed=1)
        progress.complete_step("a")

        # then
        assert "  a detail" in capture_rendered(progress)

    def test_no_details_renders_only_step_rows(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])

        # when
        rendered = capture_rendered(progress)

        # then
        assert len([line for line in rendered.split("\n") if line]) == 1

    def test_replacing_details_discards_the_previous_lines(self) -> None:
        # given
        progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])
        progress.set_step_details("a", [Text("stale detail")])

        # when
        progress.set_step_details("a", [Text("fresh detail")])

        # then
        rendered = capture_rendered(progress)
        assert "stale detail" not in rendered
        assert "  fresh detail" in rendered

    def test_updater_sets_details_on_its_own_step(self) -> None:
        # given
        progress = MultiStepProgress(
            [StepDefinition("a", "UPLOAD"), StepDefinition("b", "RENDER")]
        )

        # when
        progress.step_progress_updater("b").set_details([Text("a detail")])

        # then
        lines = [line for line in capture_rendered(progress).split("\n") if line]
        assert "RENDER" in lines[1]
        assert lines[2] == "  a detail"

    def test_details_print_indented_under_the_step_line_without_a_tty(
        self, capsys
    ) -> None:
        # given
        console = _non_terminal_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])
            with progress.display():
                progress.start_step("a")
                # when
                progress.set_step_details(
                    "a", [Text("first detail"), Text("second detail")]
                )

        # then
        lines = [ln for ln in capsys.readouterr().out.split("\n") if ln]
        assert lines[0] == f"❯ Step 1/1 - UPLOAD - {_SPINNER_FRAMES[0]} Running..."
        assert lines[1].endswith("first detail")
        assert lines[1].startswith(" ")
        assert lines[2].endswith("second detail")

    def test_details_are_not_printed_on_a_tty(self, capsys) -> None:
        # given
        console = _truecolor_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])
            with progress.display():
                # when
                progress.set_step_details("a", [Text("a detail")])

        # then
        assert "a detail" not in capsys.readouterr().out

    def test_details_are_not_printed_outside_a_display_session(self, capsys) -> None:
        # given: simulates silent mode, where progress.display() is never entered
        console = _non_terminal_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])

            # when
            progress.set_step_details("a", [Text("a detail")])

        # then
        assert capsys.readouterr().out == ""
        assert console.file.getvalue() == ""

    def test_a_long_detail_is_cropped_to_the_terminal_without_a_tty(
        self, capsys
    ) -> None:
        # given
        name = "long" * 40 + ".sql"
        console = _non_terminal_console()
        with patch.object(progress_module, "get_console", return_value=console):
            progress = MultiStepProgress([StepDefinition("a", "UPLOAD")])
            with progress.display():
                progress.start_step("a")
                # when
                progress.set_step_details("a", [Text(name)])

        # then: printed as the component renders, so a name wider than the
        # terminal is cropped to one row rather than kept whole
        printed = [ln for ln in capsys.readouterr().out.split("\n") if "long" in ln]
        assert len(printed) == 1
        assert name.startswith(printed[0].strip())
        assert name not in printed[0]


class TestProgressSession:
    def _ctx(self, silent: bool) -> AbstractContextManager:
        ctx = MagicMock()
        ctx.silent = silent
        return patch(_SILENT, return_value=ctx)

    def test_silent_skips_display(self) -> None:
        # given
        progress = MagicMock()

        # when
        with self._ctx(silent=True):
            with progress_session(progress):
                pass

        # then
        progress.display.assert_not_called()

    def test_silent_exception_fails_running_step_and_reraises(self) -> None:
        # given
        progress = MagicMock()

        # when
        with self._ctx(silent=True):
            with pytest.raises(ValueError):
                with progress_session(progress):
                    raise ValueError("boom")

        # then: state stays consistent without refreshing - a refresh on a
        # display that never started would print the checklist, breaking silence
        progress.display.assert_not_called()
        progress.fail_running.assert_called_once()
        progress.refresh.assert_not_called()

    def test_non_silent_opens_display(self) -> None:
        # given
        progress = MagicMock()

        # when
        with self._ctx(silent=False):
            with progress_session(progress):
                pass

        # then
        progress.display.assert_called_once()
        progress.fail_running.assert_not_called()

    def test_exception_fails_running_step_and_reraises(self) -> None:
        # given
        progress = MagicMock()

        # when
        with self._ctx(silent=False):
            with pytest.raises(ValueError):
                with progress_session(progress):
                    raise ValueError("boom")

        # then
        progress.fail_running.assert_called_once()
        progress.refresh.assert_called_once()
