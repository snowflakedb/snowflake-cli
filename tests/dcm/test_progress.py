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
"""Tests for the DCM progress building blocks.

Generic step rendering/lifecycle is covered by tests/dcm/test_multistep_progress.py;
this module exercises the DCM-specific pieces: the server-poll driver's phase
mapping, and the file-upload counter.

ServerPoll's only public surface is __init__ and run(); every test here drives
it through run(), never touching its private helper methods.
"""

import logging
from typing import Any, Dict, Optional, Sequence, Tuple
from unittest.mock import MagicMock, patch

import pytest
from rich.spinner import Spinner
from snowflake.cli._plugins.dcm.multistep_progress import (
    MultiStepProgress,
    StepDefinition,
    StepState,
)
from snowflake.cli._plugins.dcm.progress import (
    _NO_DATA_MAX_RETRY,
    COMPILE,
    DEPLOY,
    PLAN,
    PURGE,
    RENDER,
    FileUploadProgress,
    ServerPoll,
    _poll_interval_seconds,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import QueryStatus

from tests.dcm.multi_step_progress_capture import find_line

_SLEEP = "snowflake.cli._plugins.dcm.progress.time.sleep"
_SPINNER_FRAMES = Spinner("dots").frames
TEST_SFQID = "af72f4cc-107c-4f1b-b8a9-7a9811203bc5"


def _conn(
    *,
    still_running: Sequence[bool],
    is_error: bool,
    rows: Sequence[Optional[list]] = (),
) -> Tuple[MagicMock, MagicMock]:
    conn = MagicMock()
    conn.is_still_running.side_effect = still_running
    conn.is_an_error.return_value = is_error
    poll_count = sum(1 for value in still_running if value)
    padded_rows = list(rows) + [None] * (poll_count - len(rows))
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.fetchone.side_effect = padded_rows
    conn.cursor.return_value = cursor
    return conn, cursor


def _snapshot(
    progress: MultiStepProgress, steps: Sequence[StepDefinition]
) -> Dict[str, StepState]:
    return {step.key: progress.step_state(step.key) for step in steps}


class TestServerPollRun:
    _STEPS = [RENDER, COMPILE, PLAN, DEPLOY]

    def test_success_completes_all_steps_and_returns_result_cursor(self) -> None:
        # given
        conn, cursor = _conn(
            still_running=[True, False],
            is_error=False,
            rows=[['{"phase": "PLAN", "progress": 50}']],
        )
        progress = MultiStepProgress(self._STEPS)
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)

        # when
        with patch(_SLEEP):
            result = poll.run()

        # then
        assert result is cursor
        cursor.get_results_from_sfqid.assert_called_once_with(TEST_SFQID)
        for step in self._STEPS:
            assert progress.step_state(step.key) == StepState.DONE

    def test_failure_marks_non_terminal_steps_failed(self) -> None:
        # given
        conn, cursor = _conn(still_running=[False], is_error=True)
        progress = MultiStepProgress(self._STEPS)
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)

        # when
        result = poll.run()

        # then
        assert result is cursor
        for step in self._STEPS:
            assert progress.step_state(step.key) == StepState.FAILED

    def test_fetches_query_status_once_per_loop_boundary(self) -> None:
        # given: loop runs for 2 iterations (True, False) then checks success/failure
        conn, _ = _conn(still_running=[True, False], is_error=False)
        progress = MultiStepProgress(self._STEPS)
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)

        # when
        with patch(_SLEEP):
            poll.run()

        # then: one status fetch per is_still_running check (2), and the trailing
        # success/failure check reuses the last fetched status with no extra call
        assert conn.get_query_status.call_count == 2


class TestServerPollProgressQuery:
    _STEPS = [RENDER, COMPILE, PLAN, DEPLOY]

    def test_sfqid_is_bound_not_interpolated(self) -> None:
        # given
        conn, cursor = _conn(still_running=[True, False], is_error=False)
        poll = ServerPoll(conn, MultiStepProgress(self._STEPS), self._STEPS, TEST_SFQID)

        # when
        with patch(_SLEEP):
            poll.run()

        # then
        cursor.execute.assert_called_once_with(
            "SELECT SYSTEM$GET_DCM_PROJECT_PROGRESS(?)",
            (TEST_SFQID,),
            _force_qmark_paramstyle=True,
        )


class TestServerPollIgnoresUnusablePolls:
    """A poll that yields no usable phase data must leave every step's state
    where the unconditional first-step start left it — covers missing/malformed
    rows, JSON that fails schema validation, and phases the caller isn't
    tracking. All of these are swallowed identically by ServerPoll internally,
    so from the outside they're indistinguishable; a single parametrized test
    covers them.
    """

    _STEPS = [RENDER, COMPILE, PLAN, DEPLOY]

    @pytest.mark.parametrize(
        "row",
        [
            None,
            [None],
            ["not valid json"],
            ['{"phase": 42, "progress": 0}'],
            ['{"phase": "", "progress": 0}'],
            ['{"progress": 0}'],
            ['{"phase": "BUILD", "progress": 50}'],
        ],
        ids=[
            "no_row",
            "null_row",
            "malformed_json",
            "non_string_phase",
            "empty_phase",
            "missing_phase_key",
            "unrecognized_phase",
        ],
    )
    def test_state_is_unchanged(self, row: Optional[list]) -> None:
        # given
        progress = MultiStepProgress(self._STEPS)
        conn, _ = _conn(still_running=[True, False], is_error=False, rows=[row])
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)
        snapshot: Dict[str, StepState] = {}

        def _capture(*_args: object) -> None:
            snapshot.update(_snapshot(progress, self._STEPS))

        # when
        with patch(_SLEEP, side_effect=_capture):
            poll.run()

        # then
        assert snapshot == {
            RENDER.key: StepState.RUNNING,
            COMPILE.key: StepState.PENDING,
            PLAN.key: StepState.PENDING,
            DEPLOY.key: StepState.PENDING,
        }

    def test_unrecognized_phase_logs_a_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # given
        progress = MultiStepProgress(self._STEPS)
        conn, _ = _conn(
            still_running=[True, False],
            is_error=False,
            rows=[['{"phase": "BUILD", "progress": 50}']],
        )
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)

        # when
        with caplog.at_level(logging.WARNING):
            with patch(_SLEEP):
                poll.run()

        # then
        assert "BUILD" in caplog.text
        assert TEST_SFQID in caplog.text


class TestServerPoll:
    """The driver maps the server's reported phase onto the progress steps."""

    _STEPS = [RENDER, COMPILE, PLAN, DEPLOY]

    def test_first_step_runs_before_any_progress_report(self) -> None:
        # given: the server reports nothing usable for the whole operation
        progress = MultiStepProgress(self._STEPS)
        conn, _ = _conn(still_running=[True, False], is_error=False, rows=[None])
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)
        snapshot: Dict[str, Any] = {}

        def _capture(*_args: object) -> None:
            snapshot["state"] = progress.step_state(RENDER.key)
            snapshot["line"] = find_line(progress, RENDER.label)

        # when
        with patch(_SLEEP, side_effect=_capture):
            poll.run()

        # then: the first step spins rather than the checklist looking stalled
        assert snapshot["state"] == StepState.RUNNING
        assert any(frame in snapshot["line"] for frame in _SPINNER_FRAMES)

    def test_first_step_runs_even_when_never_polled(self) -> None:
        # given: the query already finished by the first status check
        progress = MultiStepProgress(self._STEPS)
        conn, _ = _conn(still_running=[False], is_error=False)
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)

        # when
        poll.run()

        # then: started, then completed by finalization like every other step
        for step in self._STEPS:
            assert progress.step_state(step.key) == StepState.DONE

    def test_known_phase_advances_state(self) -> None:
        # given
        progress = MultiStepProgress(self._STEPS)
        conn, _ = _conn(
            still_running=[True, False],
            is_error=False,
            rows=[['{"phase": "PLAN", "progress": 25}']],
        )
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)
        snapshot: Dict[str, Any] = {}

        def _capture(*_args: object) -> None:
            snapshot.update(_snapshot(progress, self._STEPS))
            snapshot["plan_line"] = find_line(progress, "PLAN")

        # when
        with patch(_SLEEP, side_effect=_capture):
            poll.run()

        # then
        assert snapshot["RENDER"] == StepState.DONE
        assert snapshot["COMPILE"] == StepState.DONE
        assert snapshot["PLAN"] == StepState.RUNNING
        assert snapshot["DEPLOY"] == StepState.PENDING
        assert "25%" in snapshot["plan_line"]

    def test_indeterminate_phase_starts_without_progress_bar(self) -> None:
        # given
        progress = MultiStepProgress(self._STEPS)
        conn, _ = _conn(
            still_running=[True, False],
            is_error=False,
            rows=[['{"phase": "RENDER", "progress": 0}']],
        )
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)
        snapshot: Dict[str, Any] = {}

        def _capture(*_args: object) -> None:
            snapshot["state"] = progress.step_state("RENDER")
            snapshot["line"] = find_line(progress, "RENDER")

        # when
        with patch(_SLEEP, side_effect=_capture):
            poll.run()

        # then
        assert snapshot["state"] == StepState.RUNNING
        assert "%" not in snapshot["line"]

    def test_phase_switches_to_progress_bar_once_progress_is_nonzero(self) -> None:
        # given: same phase, first polled with no progress reported yet, then with progress
        progress = MultiStepProgress(self._STEPS)
        conn, _ = _conn(
            still_running=[True, True, False],
            is_error=False,
            rows=[
                ['{"phase": "PLAN", "progress": 0}'],
                ['{"phase": "PLAN", "progress": 40}'],
            ],
        )
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)
        plan_lines = []

        def _capture(*_args: object) -> None:
            plan_lines.append(find_line(progress, "PLAN"))

        # when
        with patch(_SLEEP, side_effect=_capture):
            poll.run()

        # then
        assert "%" not in plan_lines[0]
        assert "40%" in plan_lines[1]

    def test_unrecognized_phase_does_not_disturb_already_advanced_steps(self) -> None:
        # given: a normal poll advances PLAN, then a later poll reports a phase
        # name the CLI doesn't recognize (e.g. server/CLI version skew)
        progress = MultiStepProgress(self._STEPS)
        conn, _ = _conn(
            still_running=[True, True, False],
            is_error=False,
            rows=[
                ['{"phase": "PLAN", "progress": 25}'],
                ['{"phase": "SOMETHING_NEW", "progress": 99}'],
            ],
        )
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)
        snapshots = []

        def _capture(*_args: object) -> None:
            snapshots.append(_snapshot(progress, self._STEPS))

        # when
        with patch(_SLEEP, side_effect=_capture):
            poll.run()

        # then: state after the unrecognized-phase poll is unchanged from before it
        assert snapshots[1] == snapshots[0]
        assert snapshots[1]["PLAN"] == StepState.RUNNING

    def test_finalize_failure_marks_all_non_terminal(self) -> None:
        # given: a poll reports COMPILE running (RENDER completes naturally as a
        # result; PLAN/DEPLOY never start)
        progress = MultiStepProgress(self._STEPS)
        conn, _ = _conn(
            still_running=[True, False],
            is_error=True,
            rows=[['{"phase": "COMPILE", "progress": 0}']],
        )
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)

        # when
        with patch(_SLEEP):
            poll.run()

        # then
        assert progress.step_state("RENDER") == StepState.DONE
        assert progress.step_state("COMPILE") == StepState.FAILED
        assert progress.step_state("PLAN") == StepState.FAILED
        assert progress.step_state("DEPLOY") == StepState.FAILED


class TestServerPollPurgePhases:
    """``purge`` tracks PLAN and DEPLOY only - it uploads nothing, so there is
    no UPLOAD, RENDER or COMPILE - and displays the DEPLOY phase as PURGE."""

    _SERVER_STEPS = [PLAN, PURGE]

    def test_deploy_phase_advances_state_and_is_labeled_purge(self) -> None:
        # given
        progress = MultiStepProgress(self._SERVER_STEPS)
        conn, _ = _conn(
            still_running=[True, False],
            is_error=False,
            rows=[['{"phase": "DEPLOY", "progress": 40}']],
        )
        poll = ServerPoll(conn, progress, self._SERVER_STEPS, TEST_SFQID)
        snapshot: Dict[str, Any] = {}

        def _capture(*_args: object) -> None:
            snapshot.update(_snapshot(progress, self._SERVER_STEPS))
            snapshot["purge_line"] = find_line(progress, "PURGE")

        # when
        with patch(_SLEEP, side_effect=_capture):
            poll.run()

        # then
        assert snapshot["PLAN"] == StepState.DONE
        assert snapshot["DEPLOY"] == StepState.RUNNING
        assert "40%" in snapshot["purge_line"]

    def test_finalize_success_completes_all_steps(self) -> None:
        # given
        progress = MultiStepProgress(self._SERVER_STEPS)
        conn, _ = _conn(
            still_running=[True, False],
            is_error=False,
            rows=[['{"phase": "DEPLOY", "progress": 40}']],
        )
        poll = ServerPoll(conn, progress, self._SERVER_STEPS, TEST_SFQID)

        # when
        with patch(_SLEEP):
            poll.run()

        # then
        for step in self._SERVER_STEPS:
            assert progress.step_state(step.key) == StepState.DONE


class TestFileUploadProgress:
    """The DCM file-by-file upload counter."""

    def _upload(self, total: int) -> Tuple[MultiStepProgress, FileUploadProgress]:
        progress = MultiStepProgress(
            [StepDefinition("upload", "Uploading project files")]
        )
        return progress, FileUploadProgress(
            progress.step_progress_updater("upload"), total
        )

    def test_construction_starts_running_step_with_progress_bar(self) -> None:
        # given / when
        progress, _ = self._upload(3)

        # then
        assert progress.step_state("upload") == StepState.RUNNING
        assert "0%" in find_line(progress, "Uploading project files")

    def test_advance_progresses_and_caps_at_full(self) -> None:
        # given
        progress, upload = self._upload(2)

        # when
        upload.advance()
        # then
        assert "50%" in find_line(progress, "Uploading project files")

        # when
        upload.advance()
        upload.advance()
        # then
        assert "100%" in find_line(progress, "Uploading project files")


class TestServerPollUnavailableStatus:
    """A query whose status Snowflake never reports cannot be waited on: the
    poll has nothing to observe, so it must stop rather than spin forever.
    Long-running queries are unaffected - the wait itself is unbounded."""

    _STEPS = [RENDER, COMPILE, PLAN, DEPLOY]

    def _conn_reporting(
        self, statuses: Sequence[QueryStatus]
    ) -> Tuple[MagicMock, MagicMock]:
        conn = MagicMock()
        conn.get_query_status.side_effect = list(statuses)
        conn.is_still_running.side_effect = SnowflakeConnection.is_still_running
        conn.is_an_error.side_effect = SnowflakeConnection.is_an_error
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.fetchone.return_value = None
        conn.cursor.return_value = cursor
        return conn, cursor

    def test_status_never_reported_stops_with_query_id(self) -> None:
        # given
        conn, _ = self._conn_reporting([QueryStatus.NO_DATA] * (_NO_DATA_MAX_RETRY + 5))
        poll = ServerPoll(conn, MultiStepProgress(self._STEPS), self._STEPS, TEST_SFQID)

        # when
        with patch(_SLEEP):
            with pytest.raises(CliError) as err:
                poll.run()

        # then
        assert TEST_SFQID in str(err.value)
        assert "no status" in str(err.value)

    def test_transient_missing_status_does_not_stop_the_wait(self) -> None:
        # given: NO_DATA right up to the cap, then the query becomes visible
        conn, cursor = self._conn_reporting(
            [QueryStatus.NO_DATA] * _NO_DATA_MAX_RETRY
            + [QueryStatus.RUNNING, QueryStatus.NO_DATA, QueryStatus.SUCCESS]
        )
        poll = ServerPoll(conn, MultiStepProgress(self._STEPS), self._STEPS, TEST_SFQID)

        # when
        with patch(_SLEEP):
            result = poll.run()

        # then: the counter resets, so only *consecutive* silence ends the wait
        assert result is cursor

    def test_running_query_is_waited_on_indefinitely(self) -> None:
        # given: far more RUNNING polls than the NO_DATA cap allows
        conn, cursor = self._conn_reporting(
            [QueryStatus.RUNNING] * (_NO_DATA_MAX_RETRY * 10) + [QueryStatus.SUCCESS]
        )
        poll = ServerPoll(conn, MultiStepProgress(self._STEPS), self._STEPS, TEST_SFQID)

        # when
        with patch(_SLEEP):
            result = poll.run()

        # then
        assert result is cursor


class TestPollIntervalSeconds:
    @pytest.mark.parametrize(
        "elapsed, expected",
        [
            (0, 1),
            (59, 1),
            (60, 10),
            (600, 10),
        ],
    )
    def test_poll_interval_seconds(self, elapsed: int, expected: int) -> None:
        # given / when / then
        assert _poll_interval_seconds(elapsed) == expected
