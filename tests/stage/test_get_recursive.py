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
import json
import re
import threading
from pathlib import Path, PurePosixPath
from unittest import mock

import pytest
from snowflake.cli._plugins.stage.manager import (
    STAGE_UPLOAD_WORKERS_ENV_VAR,
    StageManager,
)
from snowflake.connector import ProgrammingError

from tests.testing_utils.files_and_dirs import assert_file_permissions_are_strict

STAGE_MANAGER = "snowflake.cli._plugins.stage.manager.StageManager"
_COLUMNS = ["file", "size", "status", "message"]


def _list_rows(relative_paths: list[str], prefix: str = "mystage") -> list[dict]:
    return [{"name": f"{prefix}/{relative}"} for relative in relative_paths]


def _get_statements(mock_execute) -> list[str]:
    return [
        call.args[0]
        for call in mock_execute.mock_calls
        if call.args and str(call.args[0]).startswith("get ")
    ]


def _take_sql_atom(text: str) -> tuple[str, str]:
    text = text.lstrip()
    if text.startswith("'"):
        chars: list[str] = []
        index = 1
        while index < len(text):
            if text[index] == "'" and index + 1 < len(text) and text[index + 1] == "'":
                chars.append("'")
                index += 2
                continue
            if text[index] == "'":
                return "".join(chars), text[index + 1 :]
            chars.append(text[index])
            index += 1
        raise AssertionError(f"unterminated SQL literal in {text!r}")
    token, _, rest = text.partition(" ")
    return token, rest


def _parse_get(query: str) -> tuple[str, Path, str | None]:
    assert query.startswith("get "), query
    rest = query[len("get ") :]
    stage, rest = _take_sql_atom(rest)
    uri, rest = _take_sql_atom(rest)
    pattern = None
    if "pattern=" in rest:
        pattern, _ = _take_sql_atom(rest.partition("pattern=")[2])
    assert uri.startswith("file://"), uri
    return stage, Path(uri[len("file://") :]), pattern


def _files_for_get(
    stage: str, pattern: str | None, files: list[str], *, stage_root: str = "@mystage"
) -> list[str]:
    """Files a Snowflake GET of ``stage`` would return, including nested ones.

    PATTERN must match the user path, the stage-prefixed path, and a path with
    an internal prefix. A directory GET with no PATTERN returns every file
    under that prefix, nested files included.
    """
    if not stage.startswith(stage_root):
        raise AssertionError(stage)
    suffix = stage[len(stage_root) :].lstrip("/")
    if not stage.endswith("/"):
        return [relative for relative in files if relative == suffix]
    chosen = []
    for relative in files:
        if suffix and not relative.startswith(suffix):
            continue
        if pattern is None:
            chosen.append(relative)
            continue
        representations = [
            relative,
            f"mystage/{relative}",
            f"internal/prefix/{relative}",
        ]
        if all(re.fullmatch(pattern, item) for item in representations):
            chosen.append(relative)
    return chosen


def _download_tree(mock_execute, mock_cursor, dest: Path, files: list[str]) -> None:
    """Run get_recursive and let each GET write the files Snowflake would."""
    writes: dict[Path, str] = {}
    lock = threading.Lock()

    def execute(query, **kwargs):
        if str(query).split()[0].lower() == "ls":
            return mock_cursor(_list_rows(files), [])
        stage, local, pattern = _parse_get(query)
        for relative in _files_for_get(stage, pattern, files):
            target = local / PurePosixPath(relative).name
            with lock:
                previous = writes.get(target)
                if previous is not None and previous != relative:
                    raise AssertionError(
                        f"{target} received {previous!r} and {relative!r}"
                    )
                writes[target] = relative
            target.write_text(relative)
        return mock_cursor([], _COLUMNS)

    mock_execute.side_effect = execute
    StageManager().get_recursive("@mystage", dest)


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_many_files_in_one_directory_use_one_get(mock_execute, mock_cursor, tmp_path):
    files = [f"definitions/f{index}.sql" for index in range(600)]
    mock_execute.return_value = mock_cursor(_list_rows(files), [])

    StageManager().get_recursive("@mystage", tmp_path)

    gets = _get_statements(mock_execute)
    assert len(gets) == 1
    stage_sql = gets[0].split(" file://", 1)[0]
    assert stage_sql == "get @mystage/definitions/"
    assert "f0.sql" not in stage_sql
    assert "f599.sql" not in stage_sql


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_files_split_across_two_directories_use_two_gets(
    mock_execute, mock_cursor, tmp_path
):
    files = ["left/a.sql", "left/b.sql", "right/c.sql"]
    mock_execute.return_value = mock_cursor(_list_rows(files), [])

    StageManager().get_recursive("@mystage", tmp_path)

    stages = [query.split(" file://", 1)[0] for query in _get_statements(mock_execute)]
    assert sorted(stages) == ["get @mystage/left/", "get @mystage/right/"]


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_nested_tree_lands_at_the_same_relative_paths(
    mock_execute, mock_cursor, tmp_path
):
    files = [
        "plan_result.json",
        "rendered/notes.txt",
        "rendered/sources/definitions/one.sql",
        "rendered/sources/definitions/two.sql",
    ]
    dest = tmp_path / "out"

    _download_tree(mock_execute, mock_cursor, dest, files)

    found = {
        path.relative_to(dest).as_posix(): path.read_text()
        for path in dest.rglob("*")
        if path.is_file()
    }
    assert found == {relative: relative for relative in files}
    assert len(_get_statements(mock_execute)) == 3


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_same_basename_in_a_subdirectory_is_not_written_at_the_root(
    mock_execute, mock_cursor, tmp_path
):
    files = ["README.md", "dir/README.md"]
    dest = tmp_path / "out"

    _download_tree(mock_execute, mock_cursor, dest, files)

    assert (dest / "README.md").read_text() == "README.md"
    assert (dest / "dir" / "README.md").read_text() == "dir/README.md"
    assert len(_get_statements(mock_execute)) == 2


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_repeated_directory_name_keeps_each_file_at_its_relative_path(
    mock_execute, mock_cursor, tmp_path
):
    files = ["src/util.sql", "src/pkg/src/util.sql"]
    dest = tmp_path / "out"

    _download_tree(mock_execute, mock_cursor, dest, files)

    assert (dest / "src" / "util.sql").read_text() == "src/util.sql"
    assert (dest / "src" / "pkg" / "src" / "util.sql").read_text() == (
        "src/pkg/src/util.sql"
    )


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_single_file_download_does_not_get_the_parent_directory(
    mock_execute, mock_cursor, tmp_path
):
    mock_execute.return_value = mock_cursor(
        [{"name": "mystage/a/b/only.sql"}],
        [],
    )

    StageManager().get_recursive("@mystage/a/b/only.sql", tmp_path)

    gets = _get_statements(mock_execute)
    assert len(gets) == 1
    assert gets[0].split(" file://", 1)[0] == "get @mystage/a/b/only.sql"


@pytest.mark.parametrize(
    (
        "budget",
        "parallel",
        "directory_count",
        "expected_in_flight",
        "expected_parallel",
    ),
    [
        (1, 1, 4, 1, 1),
        (32, 1, 40, 32, 1),
        (8, 3, 6, 2, 3),
        (4, 16, 4, 1, 4),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_directory_gets_stay_within_the_worker_budget(
    mock_execute,
    mock_cursor,
    tmp_path,
    monkeypatch,
    budget,
    parallel,
    directory_count,
    expected_in_flight,
    expected_parallel,
):
    monkeypatch.setenv(STAGE_UPLOAD_WORKERS_ENV_VAR, str(budget))
    files = [f"d{index:02d}/f.sql" for index in range(directory_count)]
    release = threading.Event()
    condition = threading.Condition()
    state = {"in_flight": 0, "max": 0, "parallels": []}

    def execute(query, **kwargs):
        if str(query).split()[0].lower() == "ls":
            return mock_cursor(_list_rows(files), [])
        match = re.search(r"parallel=(\d+)", query)
        assert match is not None, query
        with condition:
            state["in_flight"] += 1
            if not release.is_set():
                state["max"] = max(state["max"], state["in_flight"])
            state["parallels"].append(int(match.group(1)))
            condition.notify_all()
        if not release.wait(timeout=5):
            raise TimeoutError("directory GET was not released")
        with condition:
            state["in_flight"] -= 1
        return mock_cursor([("f.sql", 1, "DOWNLOADED", "")], _COLUMNS)

    mock_execute.side_effect = execute
    errors: list[BaseException] = []

    def _run():
        try:
            StageManager().get_recursive("@mystage", tmp_path, parallel=parallel)
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=_run)
    worker.start()
    try:
        with condition:
            reached = condition.wait_for(
                lambda: state["in_flight"] >= expected_in_flight,
                timeout=5,
            )
    finally:
        release.set()
    worker.join(timeout=5)

    assert not errors
    assert not worker.is_alive()
    assert reached
    assert state["max"] == expected_in_flight
    assert set(state["parallels"]) == {expected_parallel}
    assert state["max"] * expected_parallel <= budget


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_failed_get_exits_nonzero_and_keeps_a_file_already_written(
    mock_execute, mock_cursor, runner, tmp_path, monkeypatch
):
    monkeypatch.setenv(STAGE_UPLOAD_WORKERS_ENV_VAR, "1")
    dest = tmp_path / "out"
    kept = dest / "a_keep" / "ok.sql"

    def execute(query, **kwargs):
        if str(query).split()[0].lower() == "ls":
            return mock_cursor(
                _list_rows(["a_keep/ok.sql", "b_fail/no.sql"]),
                [],
            )
        if "a_keep" in query:
            kept.parent.mkdir(parents=True, exist_ok=True)
            kept.write_text("kept")
            kept.chmod(0o666)
            return mock_cursor([("ok.sql", 1, "DOWNLOADED", "")], _COLUMNS)
        raise ProgrammingError("GET failed")

    mock_execute.side_effect = execute
    result = runner.invoke(
        ["stage", "copy", "@mystage", str(dest), "--recursive"],
    )

    assert result.exit_code != 0, result.output
    assert "GET failed" in result.output
    assert kept.read_text() == "kept"
    assert_file_permissions_are_strict(kept)


def _rows_for_directory(query: str) -> list[tuple]:
    if "/dir_a/" in query:
        return [
            ("c.sql", 1, "DOWNLOADED", ""),
            ("a.sql", 1, "DOWNLOADED", ""),
        ]
    if "/dir_b/" in query:
        return [("b.sql", 1, "DOWNLOADED", "")]
    raise AssertionError(query)


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_copy_recursive_returns_one_sorted_row_per_file(
    mock_execute, mock_cursor, runner, tmp_path
):
    def execute(query, **kwargs):
        if str(query).split()[0].lower() == "ls":
            return mock_cursor(
                _list_rows(["dir_b/b.sql", "dir_a/c.sql", "dir_a/a.sql"]),
                [],
            )
        return mock_cursor(_rows_for_directory(query), _COLUMNS)

    mock_execute.side_effect = execute
    result = runner.invoke(
        ["stage", "copy", "@mystage", str(tmp_path), "--recursive", "--format", "JSON"]
    )

    assert result.exit_code == 0, result.output
    rows = json.loads(result.output)
    assert [row["file"] for row in rows] == ["a.sql", "b.sql", "c.sql"]
    assert len(rows) == 3


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_git_copy_returns_one_sorted_row_per_file(
    mock_execute, mock_cursor, runner, tmp_path
):
    names = [
        "repo/branches/main/dir_b/b.sql",
        "repo/branches/main/dir_a/c.sql",
        "repo/branches/main/dir_a/a.sql",
    ]

    def execute(query, **kwargs):
        if str(query).split()[0].lower() == "ls":
            return mock_cursor([{"name": name} for name in names], [])
        return mock_cursor(_rows_for_directory(query), _COLUMNS)

    mock_execute.side_effect = execute
    result = runner.invoke(
        [
            "git",
            "copy",
            "@repo/branches/main/",
            str(tmp_path),
            "--format",
            "JSON",
        ]
    )

    assert result.exit_code == 0, result.output
    rows = json.loads(result.output)
    assert [row["file"] for row in rows] == ["a.sql", "b.sql", "c.sql"]
    assert len(rows) == 3
