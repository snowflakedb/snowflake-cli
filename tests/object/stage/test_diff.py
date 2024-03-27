import hashlib
from pathlib import Path
from typing import Dict, List, Set, Tuple, Union
from unittest import mock

import pytest
from click.exceptions import (
    ClickException,
    FileError,
)
from snowflake.cli.api.exceptions import (
    SnowflakeSQLExecutionError,
)
from snowflake.cli.plugins.object.stage.diff import (
    DiffResult,
    _filter_from_diff,
    _get_full_file_paths_to_sync,
    delete_only_on_stage_files,
    enumerate_files,
    get_stage_path_from_file,
    put_files_on_stage,
    stage_diff,
    sync_local_diff_with_stage,
)
from snowflake.cli.plugins.object.stage.manager import StageManager

from tests.testing_utils.files_and_dirs import temp_local_dir

STAGE_MANAGER = "snowflake.cli.plugins.object.stage.manager.StageManager"
STAGE_DIFF = "snowflake.cli.plugins.object.stage.diff"

FILE_CONTENTS = {
    "README.md": "This is a README\n",
    "ui/streamlit.py": "# this is a streamlit\n",
    "my.jar": b"083hgia2r92tha",
}
DEFAULT_LAST_MODIFIED = "Tue, 5 Sep 2023 17:59:21 GMT"
STAGE_LS_COLUMNS = ["name", "size", "md5", "last_modified"]


def md5_of(contents: Union[str, bytes]) -> str:
    hash_value = hashlib.md5()
    if isinstance(contents, bytes):
        hash_value.update(contents)
    else:
        hash_value.update(contents.encode("UTF-8"))
    return hash_value.hexdigest()


def stage_contents(
    files: Dict[str, Union[str, bytes]], last_modified: str = DEFAULT_LAST_MODIFIED
) -> List[Tuple[str, int, str, str]]:
    """
    Return file contents as they would be listed by a SNOWFLAKE_SSE stage
    if they were uploaded with the given structure and contents.
    """
    return [
        (f"stage/{relpath}", len(contents), md5_of(contents), last_modified)
        for (relpath, contents) in files.items()
    ]


@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_empty_stage(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(rows=[], columns=STAGE_LS_COLUMNS)

    with temp_local_dir(FILE_CONTENTS) as local_path:
        diff_result = stage_diff(local_path, "a.b.c")
        assert len(diff_result.only_on_stage) == 0
        assert len(diff_result.different) == 0
        assert len(diff_result.identical) == 0
        assert sorted(diff_result.only_local) == sorted(FILE_CONTENTS.keys())


@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_empty_dir(mock_list, mock_cursor):
    mock_list.return_value = mock_cursor(
        rows=stage_contents(FILE_CONTENTS),
        columns=STAGE_LS_COLUMNS,
    )

    with temp_local_dir({}) as local_path:
        diff_result = stage_diff(local_path, "a.b.c")
        assert sorted(diff_result.only_on_stage) == sorted(FILE_CONTENTS.keys())
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
        diff_result = stage_diff(local_path, "a.b.c")
        assert len(diff_result.only_on_stage) == 0
        assert len(diff_result.different) == 0
        assert sorted(diff_result.identical) == sorted(FILE_CONTENTS.keys())
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
        diff_result = stage_diff(local_path, "a.b.c")
        assert len(diff_result.only_on_stage) == 0
        assert len(diff_result.different) == 0
        assert sorted(diff_result.identical) == sorted(FILE_CONTENTS.keys())
        assert diff_result.only_local == ["a/new/README.md"]


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
        diff_result = stage_diff(local_path, "a.b.c")
        assert len(diff_result.only_on_stage) == 0
        assert sorted(diff_result.different) == ["README.md"]
        assert sorted(diff_result.identical) == ["my.jar", "ui/streamlit.py"]
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
            actual.append(get_stage_path_from_file(relpath))
    assert actual.sort() == expected


@mock.patch(f"{STAGE_MANAGER}.remove")
def test_delete_only_on_stage_files(mock_remove):
    stage_name = "some_stage_name"
    random_file = "some_file_on_stage"

    delete_only_on_stage_files(StageManager(), stage_name, [random_file], "some_role")
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
            stage_fqn=stage_name,
            deploy_root_path=local_path,
            files=["ui/nested/environment.yml", "README.md"],
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


@mock.patch(f"{STAGE_MANAGER}.remove")
def test_sync_local_diff_with_stage(mock_remove, other_directory):
    temp_dir = Path(other_directory)
    mock_remove.side_effect = Exception("Mock Exception")
    mock_remove.return_value = None
    diff: DiffResult = DiffResult()
    diff.only_on_stage = ["some_file_on_stage"]
    stage_name = "some_stage_name"

    with pytest.raises(SnowflakeSQLExecutionError):
        sync_local_diff_with_stage(
            role="some_role",
            deploy_root_path=temp_dir,
            diff_result=diff,
            stage_path=stage_name,
        )


def exists_mock(path: Path):
    if str(path) in ["/file", "/dir", "/dir/nested_file"]:
        return True
    else:
        return False


def is_dir_mock(path: Path):
    if str(path) == "/dir":
        return True
    else:
        return False


# Mocking Path to mimic the following directory structure:
# /file
# /dir/nested_file
@mock.patch(f"{STAGE_DIFF}.Path.is_dir", autospec=True)
@mock.patch(f"{STAGE_DIFF}.Path.exists", autospec=True)
@pytest.mark.parametrize(
    "files_to_sync,remote_paths,expected_exception",
    [
        [["file", "dir/nested_file"], set(), None],
        [["file", "file2", "dir/file3"], set(["/file2", "/dir/file3"]), None],
        [["file", "file2"], set(), FileError],
        [["dir/file3"], set(), FileError],
        [["dir"], set(), ClickException],
    ],
)
def test_get_full_file_paths_to_sync(
    path_mock_exists,
    path_mock_is_dir,
    files_to_sync: List[Path],
    remote_paths: Set[str],
    expected_exception: Exception,
):
    path_mock_exists.side_effect = exists_mock
    path_mock_is_dir.side_effect = is_dir_mock
    if expected_exception is None:
        result = _get_full_file_paths_to_sync(files_to_sync, "/", remote_paths)
        assert len(result) == len(files_to_sync)
    else:
        with pytest.raises(expected_exception):
            _get_full_file_paths_to_sync(files_to_sync, "/", remote_paths)


def test_filter_from_diff():
    diff: DiffResult = DiffResult()
    diff.different = [
        "/different",
        "/different-2",
        "/dir/different",
        "/dir/different-2",
    ]
    diff.only_local = [
        "/only_local",
        "/only_local-2",
        "/dir/only_local",
        "/dir/only_local-2",
    ]
    diff.only_on_stage = [
        "/only_on_stage",
        "/only_on_stage-2",
        "/dir/only_on_stage",
        "/dir/only_on_stage-2",
    ]

    paths_to_keep = set(
        [
            "/different",
            "/only-local",
            "/only-stage",
            "/dir/different",
            "/dir/only-local",
            "/dir/only-stage",
        ]
    )
    diff = _filter_from_diff(diff, paths_to_keep)

    for path in diff.different:
        assert path in paths_to_keep
    for path in diff.only_local:
        assert path in paths_to_keep
    for path in diff.only_on_stage:
        assert path in paths_to_keep
