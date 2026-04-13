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

from __future__ import annotations

import hashlib
import typing
from pathlib import Path
from typing import Dict, List, Union
from unittest import mock

import pytest
from snowflake.cli._plugins.stage.diff import (
    DEFAULT_UPLOAD_CONCURRENCY,
    DiffResult,
    StagePathType,
    build_md5_map,
    compute_stage_diff,
    delete_only_on_stage_files,
    enumerate_files,
    get_stage_subpath,
    preserve_from_diff,
    put_files_on_stage,
    sync_local_diff_with_stage,
)
from snowflake.cli._plugins.stage.manager import DefaultStagePathParts, StageManager
from snowflake.cli._plugins.stage.utils import print_diff_to_console
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.exceptions import (
    SnowflakeSQLExecutionError,
)

from tests.testing_utils.files_and_dirs import temp_local_dir
from tests_common import IS_WINDOWS

if IS_WINDOWS:
    pytest.skip("Does not work on Windows", allow_module_level=True)

STAGE_MANAGER = "snowflake.cli._plugins.stage.manager.StageManager"
STAGE_DIFF = "snowflake.cli._plugins.object.stage.diff"

FILE_CONTENTS = {
    "README.md": "This is a README\n",
    "ui/streamlit.py": "# this is a streamlit\n",
    "my.jar": b"083hgia2r92tha",
}
DEFAULT_LAST_MODIFIED = "Tue, 5 Sep 2023 17:59:21 GMT"
STAGE_LS_COLUMNS = ["name", "size", "md5", "last_modified"]


def as_stage_paths(paths: typing.Iterable[str]) -> List[StagePathType]:
    return [StagePathType(p) for p in paths]


def md5_of(contents: Union[str, bytes]) -> str:
    hash_value = hashlib.md5()
    if isinstance(contents, bytes):
        hash_value.update(contents)
    else:
        hash_value.update(contents.encode("UTF-8"))
    return hash_value.hexdigest()


def stage_contents(
    files: Dict[str, Union[str, bytes]], last_modified: str = DEFAULT_LAST_MODIFIED
) -> List[Dict[str, Union[str, int]]]:
    """
    Return file contents as they would be listed by a SNOWFLAKE_SSE stage
    if they were uploaded with the given structure and contents.
    """
    return [
        {
            "name": f"stage/{relpath}",
            "size": len(contents),
            "md5": md5_of(contents),
            "last_modified": last_modified,
        }
        for (relpath, contents) in files.items()
    ]


@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_empty_stage(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(rows=[], columns=STAGE_LS_COLUMNS)

    with temp_local_dir(FILE_CONTENTS) as local_path:
        diff_result = compute_stage_diff(local_path, DefaultStagePathParts("a.b.c"))
        assert len(diff_result.only_on_stage) == 0
        assert len(diff_result.different) == 0
        assert len(diff_result.identical) == 0
        assert sorted(diff_result.only_local) == sorted(
            as_stage_paths(FILE_CONTENTS.keys())
        )


@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_empty_dir(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(
        rows=stage_contents(FILE_CONTENTS),
        columns=STAGE_LS_COLUMNS,
    )

    with temp_local_dir({}) as local_path:
        diff_result = compute_stage_diff(local_path, DefaultStagePathParts("a.b.stage"))
        assert sorted(diff_result.only_on_stage) == sorted(
            as_stage_paths(FILE_CONTENTS.keys())
        )
        assert len(diff_result.different) == 0
        assert len(diff_result.identical) == 0
        assert len(diff_result.only_local) == 0


@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_identical_stage(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(
        rows=stage_contents(FILE_CONTENTS),
        columns=STAGE_LS_COLUMNS,
    )

    with temp_local_dir(FILE_CONTENTS) as local_path:
        diff_result = compute_stage_diff(local_path, DefaultStagePathParts("a.b.stage"))
        assert len(diff_result.only_on_stage) == 0
        assert len(diff_result.different) == 0
        assert sorted(diff_result.identical) == sorted(
            as_stage_paths(FILE_CONTENTS.keys())
        )
        assert len(diff_result.only_local) == 0


@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_new_local_file(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(
        rows=stage_contents(FILE_CONTENTS),
        columns=STAGE_LS_COLUMNS,
    )

    with temp_local_dir(
        {**FILE_CONTENTS, "a/new/README.md": "### I am a new markdown readme"}
    ) as local_path:
        diff_result = compute_stage_diff(local_path, DefaultStagePathParts("a.b.stage"))
        assert len(diff_result.only_on_stage) == 0
        assert len(diff_result.different) == 0
        assert sorted(diff_result.identical) == sorted(
            as_stage_paths(FILE_CONTENTS.keys())
        )
        assert diff_result.only_local == as_stage_paths(["a/new/README.md"])


@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_modified_file(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(
        rows=stage_contents(FILE_CONTENTS),
        columns=STAGE_LS_COLUMNS,
    )

    with temp_local_dir(
        {
            **FILE_CONTENTS,
            "README.md": "This is a modification to the existing README",
        }
    ) as local_path:
        diff_result = compute_stage_diff(local_path, DefaultStagePathParts("a.b.stage"))
        assert len(diff_result.only_on_stage) == 0
        assert sorted(diff_result.different) == as_stage_paths(["README.md"])
        assert sorted(diff_result.identical) == as_stage_paths(
            ["my.jar", "ui/streamlit.py"]
        )
        assert len(diff_result.only_local) == 0


@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_unmodified_file_no_remote_md5sum(mock_list, mock_cursor):

    # the stage is identical to the local, except we don't have an md5 for README.md
    rows = stage_contents(FILE_CONTENTS)
    for row in rows:
        if row["name"] == "stage/README.md":
            row["md5"] = None

    assert any([row["md5"] is None for row in rows])

    mock_list.return_value = mock_cursor(
        rows=rows,
        columns=STAGE_LS_COLUMNS,
    )

    with temp_local_dir(FILE_CONTENTS) as local_path:
        diff_result = compute_stage_diff(local_path, DefaultStagePathParts("a.b.stage"))
        assert len(diff_result.only_on_stage) == 0
        assert sorted(diff_result.different) == as_stage_paths(["README.md"])
        assert sorted(diff_result.identical) == as_stage_paths(
            ["my.jar", "ui/streamlit.py"]
        )
        assert len(diff_result.only_local) == 0


def test_get_stage_path_from_file():
    expected = [
        "",
        "ui/streamlit.py",
        "",
        "ui/nested/",
        "module-add/src/python/",
    ].sort()
    actual = []
    with temp_local_dir(
        {
            **FILE_CONTENTS,
            "ui/nested/environment.yml": "# this is a environment file\n",
            "module-add/src/python/app.py": "# this is an app file\n",
        }
    ) as local_path:
        local_files = enumerate_files(local_path)
        for local_file in local_files:
            relpath = str(local_file.relative_to(local_path))
            actual.append(get_stage_subpath(StagePathType(relpath)))
    assert actual.sort() == expected


@mock.patch(f"{STAGE_MANAGER}.remove")
def test_delete_only_on_stage_files(mock_remove):
    stage_name = "some_stage_name"
    random_file = "some_file_on_stage"

    delete_only_on_stage_files(
        StageManager(), stage_name, as_stage_paths([random_file]), "some_role"
    )
    mock_remove.assert_has_calls(
        [mock.call(stage_name=stage_name, path=random_file, role="some_role")]
    )


@mock.patch(f"{STAGE_MANAGER}.put")
@pytest.mark.parametrize("overwrite_param", [True, False])
def test_put_files_on_stage(mock_put, overwrite_param):
    stage_name = "some_stage_name"
    with temp_local_dir(
        {
            "ui/nested/environment.yml": "# this is a environment file\n",
            "README.md": "# this is an app file\n",
        }
    ) as local_path:
        put_files_on_stage(
            stage_manager=StageManager(),
            stage_root=stage_name,
            deploy_root_path=local_path,
            stage_paths=as_stage_paths(["ui/nested/environment.yml", "README.md"]),
            role="some_role",
            overwrite=overwrite_param,
        )
        expected = [
            mock.call(
                local_path=local_path / "ui/nested/environment.yml",
                stage_path=f"{stage_name}/ui/nested",  # TODO: verify if trailing slash is needed, doesn't seem so from regression tests
                role="some_role",
                overwrite=overwrite_param,
            ),
            mock.call(
                local_path=local_path / "README.md",
                stage_path=f"{stage_name}",
                role="some_role",
                overwrite=overwrite_param,
            ),
        ]
        assert mock_put.mock_calls == expected


def test_build_md5_map(mock_cursor):
    actual = build_md5_map(
        mock_cursor(
            rows=stage_contents(FILE_CONTENTS),
            columns=STAGE_LS_COLUMNS,
        ),
        DefaultStagePathParts.from_fqn("stage"),
    )

    expected = {
        StagePathType("README.md"): "9b650974f65cc49be96a5ed34ac6d1fd",
        StagePathType("my.jar"): "fc605d0e2e50cf3e71873d57f4c598b0",
        StagePathType("ui/streamlit.py"): "a7dfdfaf892ecfc5f164914123c7f2cc",
    }

    assert actual == expected


@mock.patch(f"{STAGE_MANAGER}.remove")
def test_sync_local_diff_with_stage(mock_remove, temporary_directory):
    temp_dir = Path(temporary_directory)
    mock_remove.side_effect = Exception("Mock Exception")
    mock_remove.return_value = None
    diff = DiffResult()
    diff.only_on_stage = as_stage_paths(["some_file_on_stage"])
    stage_name = "some_stage_name"

    with pytest.raises(SnowflakeSQLExecutionError):
        sync_local_diff_with_stage(
            role="some_role",
            deploy_root_path=temp_dir,
            diff_result=diff,
            stage_full_path=stage_name,
        )


def test_filter_from_diff():
    diff = DiffResult()
    diff.different = as_stage_paths(
        [
            "different",
            "different-2",
            "dir/different",
            "dir/different-2",
        ]
    )
    diff.only_local = as_stage_paths(
        [
            "only_local",
            "only_local-2",
            "dir/only_local",
            "dir/only_local-2",
        ]
    )
    diff.only_on_stage = as_stage_paths(
        [
            "only_on_stage",
            "only_on_stage-2",
            "dir/only_on_stage",
            "dir/only_on_stage-2",
        ]
    )

    paths_to_sync = as_stage_paths(
        [
            "different",
            "only-local",
            "only-stage",
            "dir/different",
            "dir/only-local",
            "dir/only-stage",
        ]
    )
    new_diff = preserve_from_diff(diff, paths_to_sync)

    for path in new_diff.different:
        assert path in paths_to_sync
    for path in new_diff.only_local:
        assert path in paths_to_sync
    for path in new_diff.only_on_stage:
        assert path in paths_to_sync
    assert new_diff.identical == diff.identical


def test_print_diff_to_console_no_bundlemap(
    capsys,
    os_agnostic_snapshot,
):
    diff = DiffResult()
    # Empty diff
    print_diff_to_console(diff)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # Only identical paths
    diff.identical = as_stage_paths(
        [
            "identical",
            "dir/identical",
        ]
    )
    print_diff_to_console(diff)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # Only deleted paths
    diff.only_on_stage = as_stage_paths(
        [
            "only_on_stage",
            "deleted",
            "dir/only_on_stage",
            "dir/deleted",
        ]
    )
    print_diff_to_console(diff)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # Only deleted and modified paths
    diff.different = as_stage_paths(
        [
            "different",
            "modified",
            "dir/different",
            "dir/modified",
        ]
    )
    print_diff_to_console(diff)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # All of deleted, modified and added paths
    diff.only_local = as_stage_paths(
        [
            "added",
            "new-file",
            "dir/added",
            "dir/new-file",
        ]
    )
    print_diff_to_console(diff)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # Only deleted and added paths
    diff.different = []
    print_diff_to_console(diff)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # Only added paths
    diff.only_on_stage = []
    print_diff_to_console(diff)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot


def test_print_diff_to_console_with_bundlemap(capsys, os_agnostic_snapshot):
    bundle_map = mock.MagicMock(spec=BundleMap, autospec=True)
    dest_to_project = {}
    bundle_map.to_project_path.side_effect = lambda dest_path: dest_to_project.get(
        dest_path
    )

    diff = DiffResult()
    # Empty diff
    print_diff_to_console(diff, bundle_map)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # Only identical paths
    diff.identical = as_stage_paths(
        [
            "identical",
            "dir/identical",
        ]
    )
    print_diff_to_console(diff, bundle_map)
    bundle_map.to_project_path.assert_not_called()
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # Only deleted paths
    diff.only_on_stage = as_stage_paths(
        [
            "only_on_stage",
            "deleted",
            "dir/only_on_stage",
            "dir/deleted",
        ]
    )
    print_diff_to_console(diff, bundle_map)
    bundle_map.to_project_path.assert_not_called()
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # Only deleted and modified paths
    diff.different = as_stage_paths(
        [
            "different",
            "modified",
            "dir/different",
            "dir/modified",
        ]
    )
    print_diff_to_console(diff, bundle_map)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    for p in diff.different:
        dest_to_project[p] = Path("src") / p
    print_diff_to_console(diff, bundle_map)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # All of deleted, modified and added paths
    diff.only_local = as_stage_paths(
        [
            "added",
            "new-file",
            "dir/added",
            "dir/new-file",
        ]
    )
    print_diff_to_console(diff, bundle_map)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    for p in diff.only_local:
        dest_to_project[p] = Path("src") / p
    print_diff_to_console(diff, bundle_map)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # Only deleted and added paths
    diff.different = []
    print_diff_to_console(diff, bundle_map)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot

    # Only added paths
    diff.only_on_stage = []
    print_diff_to_console(diff, bundle_map)
    captured = capsys.readouterr()
    assert captured.out == os_agnostic_snapshot


# ============================================================
# Tests for parallel upload / delete and temp stage optimization
# ============================================================

STAGE_DIFF_MODULE = "snowflake.cli._plugins.stage.diff"


@mock.patch(f"{STAGE_MANAGER}.put")
@pytest.mark.parametrize("overwrite_param", [True, False])
def test_put_files_on_stage_sequential_fallback(mock_put, overwrite_param):
    """When concurrency=1 (no connections), sequential path is used identically to original."""
    stage_name = "some_stage_name"
    with temp_local_dir(
        {
            "ui/nested/environment.yml": "# this is a environment file\n",
            "README.md": "# this is an app file\n",
        }
    ) as local_path:
        put_files_on_stage(
            stage_manager=StageManager(),
            stage_root=stage_name,
            deploy_root_path=local_path,
            stage_paths=as_stage_paths(["ui/nested/environment.yml", "README.md"]),
            role="some_role",
            overwrite=overwrite_param,
            connections=None,
        )
        expected = [
            mock.call(
                local_path=local_path / "ui/nested/environment.yml",
                stage_path=f"{stage_name}/ui/nested",
                role="some_role",
                overwrite=overwrite_param,
            ),
            mock.call(
                local_path=local_path / "README.md",
                stage_path=f"{stage_name}",
                role="some_role",
                overwrite=overwrite_param,
            ),
        ]
        assert mock_put.mock_calls == expected


@mock.patch(f"{STAGE_DIFF_MODULE}._put_single_file")
def test_put_files_on_stage_parallel(mock_put_single):
    """When connections are provided, files are uploaded in parallel using _put_single_file."""
    stage_name = "some_stage_name"
    mock_conn1 = mock.MagicMock()
    mock_conn2 = mock.MagicMock()
    connections = [mock_conn1, mock_conn2]

    with temp_local_dir(
        {
            "a.txt": "a",
            "b.txt": "b",
            "sub/c.txt": "c",
        }
    ) as local_path:
        put_files_on_stage(
            stage_manager=StageManager(),
            stage_root=stage_name,
            deploy_root_path=local_path,
            stage_paths=as_stage_paths(["a.txt", "b.txt", "sub/c.txt"]),
            role="some_role",
            overwrite=True,
            connections=connections,
        )
        # All 3 files should be uploaded via the parallel worker function
        assert mock_put_single.call_count == 3


@mock.patch(f"{STAGE_MANAGER}.put")
def test_put_files_on_stage_empty_list(mock_put):
    """No connections created, no put calls for empty file list."""
    mock_conn = mock.MagicMock()
    put_files_on_stage(
        stage_manager=StageManager(),
        stage_root="stage",
        deploy_root_path=Path("/tmp"),
        stage_paths=[],
        role="role",
        overwrite=False,
        connections=[mock_conn],
    )
    mock_put.assert_not_called()


@mock.patch(f"{STAGE_MANAGER}.put", side_effect=Exception("upload failed"))
def test_put_files_on_stage_parallel_partial_failure(mock_put):
    """When a parallel upload fails, SnowflakeSQLExecutionError is raised and connections still work."""
    mock_conn1 = mock.MagicMock()
    mock_conn2 = mock.MagicMock()
    connections = [mock_conn1, mock_conn2]

    with temp_local_dir({"a.txt": "a", "b.txt": "b"}) as local_path:
        with pytest.raises(SnowflakeSQLExecutionError):
            put_files_on_stage(
                stage_manager=StageManager(),
                stage_root="stage",
                deploy_root_path=local_path,
                stage_paths=as_stage_paths(["a.txt", "b.txt"]),
                role="role",
                overwrite=True,
                connections=connections,
            )


@mock.patch(f"{STAGE_DIFF_MODULE}._delete_single_file")
def test_delete_only_on_stage_files_parallel(mock_delete_single):
    """When connections are provided, deletes are parallelized via _delete_single_file."""
    mock_conn1 = mock.MagicMock()
    mock_conn2 = mock.MagicMock()
    connections = [mock_conn1, mock_conn2]

    delete_only_on_stage_files(
        StageManager(),
        "stage_name",
        as_stage_paths(["file1", "file2", "file3"]),
        "some_role",
        connections=connections,
    )
    # All 3 files should be deleted via the parallel worker function
    assert mock_delete_single.call_count == 3


@mock.patch(f"{STAGE_DIFF_MODULE}._create_new_connection")
@mock.patch(f"{STAGE_MANAGER}.remove")
@mock.patch(f"{STAGE_MANAGER}.put")
def test_sync_local_diff_with_stage_parallel(
    mock_put, mock_remove, mock_create_conn, temporary_directory
):
    """sync_local_diff_with_stage creates connection pool and closes connections after use."""
    temp_dir = Path(temporary_directory)
    mock_conn = mock.MagicMock()
    mock_create_conn.return_value = mock_conn

    diff = DiffResult()
    diff.only_local = as_stage_paths(["file1", "file2"])

    sync_local_diff_with_stage(
        role="some_role",
        deploy_root_path=temp_dir,
        diff_result=diff,
        stage_full_path="stage_name",
        concurrency=DEFAULT_UPLOAD_CONCURRENCY,
    )

    # Connections should have been created
    assert mock_create_conn.call_count == 2  # min(16, 2 total ops)
    # Connections should be closed
    assert mock_conn.close.call_count == 2


@mock.patch(f"{STAGE_DIFF_MODULE}._create_new_connection")
@mock.patch(f"{STAGE_MANAGER}.remove")
def test_sync_local_diff_with_stage_connections_closed_on_error(
    mock_remove, mock_create_conn, temporary_directory
):
    """Connection pool is cleaned up even when an error occurs."""
    temp_dir = Path(temporary_directory)
    mock_conn = mock.MagicMock()
    mock_create_conn.return_value = mock_conn
    mock_remove.side_effect = Exception("delete failed")

    diff = DiffResult()
    diff.only_on_stage = as_stage_paths(["file1", "file2"])

    with pytest.raises(SnowflakeSQLExecutionError):
        sync_local_diff_with_stage(
            role="some_role",
            deploy_root_path=temp_dir,
            diff_result=diff,
            stage_full_path="stage_name",
            concurrency=DEFAULT_UPLOAD_CONCURRENCY,
        )

    # Connections must still be closed despite the error
    assert mock_conn.close.call_count == 2


@mock.patch(f"{STAGE_MANAGER}.put")
def test_sync_local_diff_with_stage_sequential_for_single_file(
    mock_put, temporary_directory
):
    """When there's only 1 file total, no connection pool is created (sequential path)."""
    temp_dir = Path(temporary_directory)

    diff = DiffResult()
    diff.only_local = as_stage_paths(["single_file"])

    with mock.patch(f"{STAGE_DIFF_MODULE}._create_new_connection") as mock_create_conn:
        sync_local_diff_with_stage(
            role="some_role",
            deploy_root_path=temp_dir,
            diff_result=diff,
            stage_full_path="stage_name",
            concurrency=DEFAULT_UPLOAD_CONCURRENCY,
        )
        # No connections created for single file
        mock_create_conn.assert_not_called()
