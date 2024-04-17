import hashlib
from pathlib import Path
from typing import Dict, List, Union
from unittest import mock

import pytest
from snowflake.cli.api.exceptions import (
    SnowflakeSQLExecutionError,
)
from snowflake.cli.plugins.stage.diff import (
    DiffResult,
    build_md5_map,
    delete_only_on_stage_files,
    enumerate_files,
    filter_from_diff,
    get_stage_path_from_file,
    put_files_on_stage,
    stage_diff,
    sync_local_diff_with_stage,
)
from snowflake.cli.plugins.stage.manager import StageManager

from tests.nativeapp.utils import NATIVEAPP_MODULE
from tests.testing_utils.files_and_dirs import temp_local_dir

STAGE_MANAGER = "snowflake.cli.plugins.stage.manager.StageManager"
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


def test_build_md5_map(mock_cursor):
    actual = build_md5_map(
        mock_cursor(
            rows=stage_contents(FILE_CONTENTS),
            columns=STAGE_LS_COLUMNS,
        )
    )

    expected = {
        "README.md": "9b650974f65cc49be96a5ed34ac6d1fd",
        "my.jar": "fc605d0e2e50cf3e71873d57f4c598b0",
        "ui/streamlit.py": "a7dfdfaf892ecfc5f164914123c7f2cc",
    }

    assert actual == expected


@mock.patch(f"{STAGE_MANAGER}.remove")
def test_sync_local_diff_with_stage(mock_remove, other_directory):
    temp_dir = Path(other_directory)
    mock_remove.side_effect = Exception("Mock Exception")
    mock_remove.return_value = None
    diff = DiffResult()
    diff.only_on_stage = ["some_file_on_stage"]
    stage_name = "some_stage_name"

    with pytest.raises(SnowflakeSQLExecutionError):
        sync_local_diff_with_stage(
            role="some_role",
            deploy_root_path=temp_dir,
            diff_result=diff,
            stage_path=stage_name,
        )


def test_filter_from_diff():
    diff = DiffResult()
    diff.different = [
        "different",
        "different-2",
        "dir/different",
        "dir/different-2",
    ]
    diff.only_local = [
        "only_local",
        "only_local-2",
        "dir/only_local",
        "dir/only_local-2",
    ]
    diff.only_on_stage = [
        "only_on_stage",
        "only_on_stage-2",
        "dir/only_on_stage",
        "dir/only_on_stage-2",
    ]

    paths_to_keep = set(
        [
            "different",
            "only-local",
            "only-stage",
            "dir/different",
            "dir/only-local",
            "dir/only-stage",
        ]
    )
    diff = filter_from_diff(diff, paths_to_keep, True)

    for path in diff.different:
        assert path in paths_to_keep
    for path in diff.only_local:
        assert path in paths_to_keep
    for path in diff.only_on_stage:
        assert path in paths_to_keep


# When prune flag is off, remote-only files are filtered out and a warning is printed
@mock.patch(f"{NATIVEAPP_MODULE}.cc.warning")
def test_filter_from_diff_no_prune(mock_warning):
    diff = DiffResult()
    diff.only_on_stage = [
        "only-stage.txt",
        "only-stage-2.txt",
    ]

    paths_to_keep = set(["only-stage.txt"])
    diff = filter_from_diff(diff, paths_to_keep, False)

    assert len(diff.only_on_stage) == 0
    mock_warning.assert_called_once_with(
        "The following files exist only on the stage:\n['only-stage.txt']\nUse the --prune flag to delete them from the stage."
    )
