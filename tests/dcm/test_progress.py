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
from io import StringIO
from pathlib import PurePath
from typing import Any, Dict, Optional, Sequence, Tuple
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console, RenderableType
from rich.spinner import Spinner
from snowflake.cli._plugins.dcm.exceptions import QueryStatusUnavailableCliError
from snowflake.cli._plugins.dcm.models import MANIFEST_FILE_NAME, SOURCES_FOLDER
from snowflake.cli._plugins.dcm.multistep_progress import (
    MultiStepProgress,
    StepDefinition,
    StepState,
)
from snowflake.cli._plugins.dcm.progress import (
    _NO_DATA_MAX_RETRY,
    COMPILE,
    DEPLOY,
    DETAIL_BULLET,
    PLAN,
    PURGE,
    RENDER,
    FileUploadProgress,
    ServerPoll,
    UploadFolder,
    _detail_text,
    _poll_interval_seconds,
    _printable_filename,
    _upload_folders,
    upload_details,
    upload_tree,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import QueryStatus
from snowflake.connector.errors import ProgrammingError

from tests.dcm.multi_step_progress_capture import find_line

_SLEEP = "snowflake.cli._plugins.dcm.progress.time.sleep"
_SPINNER_FRAMES = Spinner("dots").frames
TEST_SFQID = "af72f4cc-107c-4f1b-b8a9-7a9811203bc5"


def _conn(
    *,
    still_running: Sequence[bool],
    is_error: bool,
    rows: Sequence[Optional[list]] = (),
    query_error: Optional[BaseException] = None,
) -> Tuple[MagicMock, MagicMock]:
    conn = MagicMock()
    conn.is_still_running.side_effect = still_running
    conn.is_an_error.return_value = is_error
    conn.get_query_status_throw_if_error.side_effect = query_error
    poll_count = sum(1 for value in still_running if value)
    padded_rows = list(rows) + [None] * (poll_count - len(rows))
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.fetchone.side_effect = padded_rows
    conn.cursor.return_value = cursor
    return conn, cursor


def _rendered_lines(*details: RenderableType, width: int = 200) -> list:
    """The plain text each detail component renders to, one entry per row.

    ``legacy_windows=False`` pins the tree's guide glyphs: rich swaps them for
    ASCII where the console cannot encode them, which would otherwise make
    these assertions depend on the host.
    """
    console = Console(file=StringIO(), width=width, legacy_windows=False)
    return [
        "".join(segment.text for segment in line).rstrip()
        for detail in details
        for line in console.render_lines(detail, pad=False)
    ]


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
        cursor.query_result.assert_called_once_with(TEST_SFQID)
        for step in self._STEPS:
            assert progress.step_state(step.key) == StepState.DONE

    def test_result_is_fetched_without_running_a_query(self) -> None:
        """``get_results_from_sfqid`` would run ``result_scan``, needing a warehouse."""
        conn, cursor = _conn(still_running=[False], is_error=False)

        ServerPoll(conn, MultiStepProgress(self._STEPS), self._STEPS, TEST_SFQID).run()

        cursor.get_results_from_sfqid.assert_not_called()
        cursor.execute.assert_not_called()

    @pytest.mark.parametrize(
        "query_error, expected, match",
        [
            (
                ProgrammingError(msg="Statement failed."),
                ProgrammingError,
                "Statement failed.",
            ),
            (None, CliError, TEST_SFQID),
        ],
        ids=["query_error_reported", "no_error_reported"],
    )
    def test_failure_raises_and_reads_no_result(
        self, query_error, expected, match
    ) -> None:
        conn, cursor = _conn(
            still_running=[False], is_error=True, query_error=query_error
        )
        progress = MultiStepProgress(self._STEPS)
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)

        with pytest.raises(expected, match=match):
            poll.run()

        for step in self._STEPS:
            assert progress.step_state(step.key) == StepState.FAILED
        cursor.query_result.assert_not_called()

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
            query_error=ProgrammingError(msg="Statement failed."),
        )
        poll = ServerPoll(conn, progress, self._STEPS, TEST_SFQID)

        # when
        with patch(_SLEEP), pytest.raises(ProgrammingError):
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
            with pytest.raises(QueryStatusUnavailableCliError) as err:
                poll.run()

        # then:
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


def _relative_paths(*paths: str) -> list:
    return [PurePath(path) for path in paths]


class TestUploadDetails:
    def test_stage_scope_line_precedes_the_file_tree(self):
        # given
        stage = FQN.from_string("MY_DB.MY_SCHEMA.DCM_PROJECT_P_123_TMP_STAGE")

        # when
        lines = _rendered_lines(
            *upload_details(stage, _relative_paths(MANIFEST_FILE_NAME))
        )

        # then
        assert (
            lines[0] == f"{DETAIL_BULLET}Create temporary stage inside MY_DB.MY_SCHEMA"
        )
        assert lines[1] == f"{DETAIL_BULLET}Upload files"
        assert lines[2].endswith(MANIFEST_FILE_NAME)

    def test_schema_only_stage_prints_just_the_schema(self):
        # given: a two-part identifier is schema.name, so there is no database
        stage = FQN.from_string("MY_SCHEMA.DCM_PROJECT_P_123_TMP_STAGE")

        # when
        lines = _rendered_lines(*upload_details(stage, []))

        # then
        assert lines == [f"{DETAIL_BULLET}Create temporary stage inside MY_SCHEMA"]

    def test_scope_is_omitted_when_the_stage_has_none(self):
        # given
        stage = FQN.from_string("DCM_PROJECT_P_123_TMP_STAGE")

        # when
        lines = _rendered_lines(*upload_details(stage, []))

        # then
        assert lines == [f"{DETAIL_BULLET}Create temporary stage"]

    def test_root_files_are_named_in_order_before_the_folders(self):
        # given
        stage = FQN.from_string("MY_DB.MY_SCHEMA.DCM_PROJECT_P_123_TMP_STAGE")

        # when
        lines = _rendered_lines(
            *upload_details(
                stage,
                _relative_paths(
                    "zzz.yml",
                    MANIFEST_FILE_NAME,
                    "README.md",
                    f"{SOURCES_FOLDER}/top.sql",
                    f"{SOURCES_FOLDER}/definitions/a.sql",
                    f"{SOURCES_FOLDER}/definitions/nested/b.sql",
                ),
            )
        )

        # then
        assert lines[1:] == [
            f"{DETAIL_BULLET}Upload files",
            "  ├─ README.md",
            f"  ├─ {MANIFEST_FILE_NAME}",
            "  ├─ zzz.yml",
            f"  └─ {SOURCES_FOLDER} (1 file)",
            "     └─ definitions (2 files)",
        ]


class TestUploadFolders:
    """Grouping of upload paths into the two-level folder structure.

    Tree rendering of that structure is covered by TestUploadTreeLines in
    tests/dcm/test_progress.py; these tests assert the structure only.
    """

    def test_no_paths_yield_no_folders(self):
        # given / when / then
        assert _upload_folders([]) == []

    def test_root_files_do_not_create_folders(self):
        # given / when
        folders = _upload_folders(_relative_paths(MANIFEST_FILE_NAME, "README.md"))

        # then
        assert folders == []

    def test_files_directly_in_a_folder_are_counted_on_it(self):
        # given / when
        folders = _upload_folders(
            _relative_paths(f"{SOURCES_FOLDER}/a.sql", f"{SOURCES_FOLDER}/b.sql")
        )

        # then
        assert folders == [UploadFolder(name=SOURCES_FOLDER, count=2, subfolders=[])]

    def test_each_subfolder_is_counted_separately(self):
        # given / when
        folders = _upload_folders(
            _relative_paths(
                f"{SOURCES_FOLDER}/top.sql",
                f"{SOURCES_FOLDER}/definitions/a.sql",
                f"{SOURCES_FOLDER}/definitions/b.sql",
                f"{SOURCES_FOLDER}/tests/t.sql",
            )
        )

        # then
        assert folders == [
            UploadFolder(
                name=SOURCES_FOLDER,
                count=1,
                subfolders=[
                    UploadFolder(name="definitions", count=2),
                    UploadFolder(name="tests", count=1),
                ],
            )
        ]

    def test_deeper_files_roll_up_into_their_subfolder(self):
        # given: nothing below the second level gets its own entry
        folders = _upload_folders(
            _relative_paths(
                f"{SOURCES_FOLDER}/definitions/nested/deep/a.sql",
                f"{SOURCES_FOLDER}/definitions/nested/b.sql",
                f"{SOURCES_FOLDER}/definitions/c.sql",
            )
        )

        # then
        assert folders == [
            UploadFolder(
                name=SOURCES_FOLDER,
                count=0,
                subfolders=[UploadFolder(name="definitions", count=3)],
            )
        ]

    def test_every_root_folder_is_grouped_not_just_sources(self):
        # given / when
        folders = _upload_folders(
            _relative_paths(
                "macros/m.sql", "macros/util/u.sql", f"{SOURCES_FOLDER}/a.sql"
            )
        )

        # then
        assert folders == [
            UploadFolder(
                name="macros",
                count=1,
                subfolders=[UploadFolder(name="util", count=1)],
            ),
            UploadFolder(name=SOURCES_FOLDER, count=1, subfolders=[]),
        ]

    def test_folders_and_subfolders_are_ordered_by_name(self):
        # given / when
        folders = _upload_folders(
            _relative_paths(
                "zebra/z.sql", "alpha/a.sql", "alpha/yak/y.sql", "alpha/bee/b.sql"
            )
        )

        # then
        assert [folder.name for folder in folders] == ["alpha", "zebra"]
        assert [sub.name for sub in folders[0].subfolders] == ["bee", "yak"]


class TestDetailText:
    def test_escape_sequences_are_stripped(self) -> None:
        # given: a file name carrying terminal control codes
        detail = _detail_text("evil\x1b[31mred\x1b[0m.sql")

        # then: the name survives, the codes do not
        assert detail.plain == "evilred.sql"

    def test_a_name_too_wide_for_the_render_is_cropped_to_one_row(self) -> None:
        # given
        detail = _detail_text("x" * 300)

        # when
        lines = _rendered_lines(detail, width=40)

        # then: cropped rather than wrapped, so a tree keeps its alignment
        assert len(lines) == 1
        assert lines[0].endswith("\u2026")


class TestPrintableFilename:
    @pytest.mark.parametrize(
        "name",
        [
            "manifest.yml",
            "my file.sql",
            "na\u00efve_caf\u00e9.sql",
            "\u0444\u0430\u0439\u043b.sql",
            "[draft].sql",
        ],
    )
    def test_an_ordinary_name_passes_through_untouched(self, name: str) -> None:
        # given / when / then
        assert _printable_filename(name) == name

    @pytest.mark.parametrize(
        "name, expected",
        [
            ("a\nb.sql", "a\\nb.sql"),
            ("a\rb.sql", "a\\rb.sql"),
            ("abc\t\t\t\t\t\tefg.sql", "abc\\t\\t\\t\\t\\t\\tefg.sql"),
            ("report\u202egnp.sql", "report\\u202egnp.sql"),
            ("in\u200bvoice.sql", "in\\u200bvoice.sql"),
            ("a\x00b.sql", "a\\x00b.sql"),
            ("a\x0e\x0fb.sql", "a\\x0e\\x0fb.sql"),
            ("full\u3000width.sql", "full\\u3000width.sql"),
        ],
    )
    def test_an_unprintable_name_is_escaped(self, name: str, expected: str) -> None:
        # given / when / then
        assert _printable_filename(name) == expected

    def test_escape_sequences_are_stripped_before_the_printable_check(self) -> None:
        # given: stripping first, so the codes vanish rather than being escaped
        assert _printable_filename("evil\x1b[31mred\x1b[0m.sql") == "evilred.sql"

    def test_a_name_from_undecodable_bytes_is_escaped(self) -> None:
        # given: pathlib surfaces bytes the filesystem encoding cannot decode as
        # surrogates, which raise UnicodeEncodeError on the way to any stream
        name = b"bad\xff\xfename.sql".decode("utf-8", "surrogateescape")

        # when
        printable = _printable_filename(name)

        # then
        assert printable == "bad\\udcff\\udcfename.sql"
        printable.encode("utf-8")

    def test_names_differing_only_in_whitespace_stay_distinct(self) -> None:
        # given: rich drops a carriage return, so without escaping these two
        # distinct names would render identically
        assert _printable_filename("abc\rd") != _printable_filename("abcd")

    def test_a_printable_backslash_is_not_doubled(self) -> None:
        # given: a backslash is an ordinary character in a posix name, so a
        # printable name is never re-escaped
        assert _printable_filename("odd\\name.sql") == "odd\\name.sql"


class TestUploadTreeEscapesNames:
    def test_a_root_file_name_is_escaped_to_one_row(self) -> None:
        # given / when
        lines = _rendered_lines(upload_tree(["evil\nfake_root.yml"], []))

        # then: without escaping the newline would open a second row and the
        # tree's guide alignment would break
        assert lines[1:] == ["\u2514\u2500 evil\\nfake_root.yml"]

    def test_a_folder_keeps_its_count_when_its_name_is_escaped(self) -> None:
        # given: tabs would otherwise expand and push the count past the crop
        lines = _rendered_lines(
            upload_tree([], [UploadFolder(name="abc\t\t\t\t\t\tefg", count=2)])
        )

        # then
        assert lines[1] == "\u2514\u2500 abc\\t\\t\\t\\t\\t\\tefg (2 files)"

    def test_the_heading_is_not_treated_as_a_name(self) -> None:
        # given / when
        lines = _rendered_lines(upload_tree(["a.yml"], []))

        # then
        assert lines[0] == "Upload files"


class TestUploadTree:
    def test_nothing_to_upload_yields_no_component(self) -> None:
        # given / when / then
        assert upload_tree([], []) is None

    def test_root_files_and_folders_render_as_a_tree(self) -> None:
        # given
        folders = [
            UploadFolder(
                name="sources",
                count=1,
                subfolders=[
                    UploadFolder(name="definitions", count=12),
                    UploadFolder(name="macros", count=4),
                    UploadFolder(name="other_files", count=4),
                    UploadFolder(name="xyz", count=1),
                ],
            )
        ]

        # when
        lines = _rendered_lines(upload_tree(["manifest.yml"], folders))

        # then
        assert lines == [
            "Upload files",
            "├─ manifest.yml",
            "└─ sources (1 file)",
            "   ├─ definitions (12 files)",
            "   ├─ macros (4 files)",
            "   ├─ other_files (4 files)",
            "   └─ xyz (1 file)",
        ]

    def test_folder_without_direct_files_shows_no_count(self) -> None:
        # given
        folders = [
            UploadFolder(name="sources", count=0, subfolders=[UploadFolder("defs", 3)])
        ]

        # when
        lines = _rendered_lines(upload_tree([], folders))

        # then
        assert lines == [
            "Upload files",
            "└─ sources",
            "   └─ defs (3 files)",
        ]

    def test_root_files_only_still_render_connectors(self) -> None:
        # given / when
        lines = _rendered_lines(upload_tree(["a.yml", "b.yml"], []))

        # then
        assert lines == [
            "Upload files",
            "├─ a.yml",
            "└─ b.yml",
        ]

    def test_square_brackets_in_a_name_are_rendered_verbatim(self) -> None:
        # given: rich would otherwise read the brackets as a markup tag
        folders = [UploadFolder(name="[draft]", count=2)]

        # when
        lines = _rendered_lines(upload_tree(["[keep].yml"], folders))

        # then
        assert lines[1] == "├─ [keep].yml"
        assert lines[2] == "└─ [draft] (2 files)"

    def test_long_names_are_not_wrapped_or_truncated(self) -> None:
        # given
        name = "x" * 300

        # when
        lines = _rendered_lines(upload_tree([name], []), width=500)

        # then: the component carries the whole name; cropping is the render's job
        assert lines[1] == f"└─ {name}"
