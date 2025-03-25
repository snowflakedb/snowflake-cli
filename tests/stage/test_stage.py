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
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock
from unittest.mock import MagicMock

import pytest
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.errno import DOES_NOT_EXIST_OR_NOT_AUTHORIZED
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

from tests_common import IS_WINDOWS

if IS_WINDOWS:
    pytest.skip("Requires further refactor to work on Windows", allow_module_level=True)


STAGE_MANAGER = "snowflake.cli._plugins.stage.manager.StageManager"

skip_python_3_12 = pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="Snowpark is not supported in Python >= 3.12"
)


@pytest.mark.parametrize(
    "stage_name, expected_stage_name",
    [("stageName", "@stageName"), ("@stageName", "@stageName")],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_list(mock_execute, runner, mock_cursor, stage_name, expected_stage_name):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["stage", "list-files", "-c", "empty", stage_name])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"ls {expected_stage_name}", cursor_class=DictCursor
    )


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_list_pattern(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(
        ["stage", "list-files", "-c", "empty", "--pattern", "REGEX", "stageName"]
    )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        "ls @stageName pattern = 'REGEX'", cursor_class=DictCursor
    )


def test_stage_list_pattern_error(runner):
    result = runner.invoke(
        ["stage", "list-files", "--pattern", "REGEX without escaped '", "stageName"]
    )
    assert result.exit_code == 1, result.output
    assert "Error" in result.output
    assert 'All "\'" characters in PATTERN must be escaped: "\\\'"' in result.output


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_list_quoted(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["stage", "list-files", "-c", "empty", '"stage name"'])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        "ls '@\"stage name\"'", cursor_class=DictCursor
    )


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_copy_remote_to_local(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            ["stage", "copy", "-c", "empty", "@stageName", str(tmp_dir)]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"get @stageName file://{Path(tmp_dir).resolve()}/ parallel=4"
    )


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_copy_remote_to_local_quoted_stage(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            ["stage", "copy", "-c", "empty", '@"stage name"', str(tmp_dir)]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"get '@\"stage name\"' file://{Path(tmp_dir).resolve()}/ parallel=4"
    )


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_copy_remote_to_local_quoted_stage_recursive(
    mock_execute, runner, mock_cursor
):
    mock_execute.side_effect = [
        mock_cursor([{"name": '"stage name"/file'}], []),
        mock_cursor([("file")], ["file"]),
    ]
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            [
                "stage",
                "copy",
                "-c",
                "empty",
                '@"stage name"',
                str(tmp_dir),
                "--recursive",
            ]
        )
    assert result.exit_code == 0, result.output
    assert mock_execute.mock_calls == [
        mock.call("ls '@\"stage name\"'", cursor_class=DictCursor),
        mock.call(
            f"get '@\"stage name\"/file' file://{Path(tmp_dir).resolve()}/ parallel=4"
        ),
    ]


@pytest.mark.parametrize(
    "raw_path,expected_uri",
    [
        ("{}/dest", "file://{}/dest/"),
        ("{}/dest.dir", "file://{}/dest.dir/"),
        ("{}/dest0", "file://{}/dest0/"),
        ("{}/dest dir", "'file://{}/dest dir/'"),
        ("{}/dest#dir", "file://{}/dest#dir/"),
        ("{}/dest*dir", "file://{}/dest*dir/"),
        ("{}/dest_?dir", "file://{}/dest_?dir/"),
        ("{}/dest%dir", "file://{}/dest%dir/"),
        ('{}/dest"dir', 'file://{}/dest"dir/'),
        ("{}/dest'dir", r"'file://{}/dest\'dir/'"),
        ("{}/dest\tdir", r"'file://{}/dest\tdir/'"),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_copy_remote_to_local_quoted_uri(
    mock_execute, runner, mock_cursor, raw_path, expected_uri
):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir).resolve()
        local_path = raw_path.replace("{}", str(tmp_dir))
        file_uri = expected_uri.replace("{}", str(tmp_dir))
        result = runner.invoke(
            ["stage", "copy", "-c", "empty", "@stageName", local_path]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(f"get @stageName {file_uri} parallel=4")


@pytest.mark.parametrize(
    "raw_path,expected_uri",
    [
        ("{}/dest", "file://{}/dest/"),
        ("{}/dest.dir", "file://{}/dest.dir/"),
        ("{}/dest0", "file://{}/dest0/"),
        ("{}/dest dir", "'file://{}/dest dir/'"),
        ("{}/dest#dir", "file://{}/dest#dir/"),
        ("{}/dest*dir", "file://{}/dest*dir/"),
        ("{}/dest_?dir", "file://{}/dest_?dir/"),
        ("{}/dest%dir", "file://{}/dest%dir/"),
        ('{}/dest"dir', 'file://{}/dest"dir/'),
        ("{}/dest'dir", r"'file://{}/dest\'dir/'"),
        ("{}/dest\tdir", r"'file://{}/dest\tdir/'"),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_copy_remote_to_local_quoted_uri_recursive(
    mock_execute, runner, mock_cursor, raw_path, expected_uri
):
    mock_execute.side_effect = [
        mock_cursor([{"name": "stageName/file.py"}], []),
        mock_cursor([(raw_path)], ["file"]),
    ]
    with TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir).resolve()
        local_path = raw_path.replace("{}", str(tmp_dir))
        file_uri = expected_uri.replace("{}", str(tmp_dir))
        result = runner.invoke(
            [
                "stage",
                "copy",
                "-c",
                "empty",
                "@stageName",
                local_path,
                "--recursive",
            ]
        )
    assert result.exit_code == 0, result.output
    assert mock_execute.mock_calls == [
        mock.call("ls @stageName", cursor_class=DictCursor),
        mock.call(f"get @stageName/file.py {file_uri} parallel=4"),
    ]


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_copy_local_to_remote(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            [
                "stage",
                "copy",
                "-c",
                "empty",
                "--overwrite",
                "--parallel",
                42,
                "--auto-compress",
                str(tmp_dir),
                "@stageName",
            ]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"put file://{Path(tmp_dir).resolve()}/* @stageName auto_compress=true parallel=42 overwrite=True",
        cursor_class=SnowflakeCursor,
    )


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_copy_local_to_remote_quoted_stage(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            [
                "stage",
                "copy",
                "-c",
                "empty",
                "--overwrite",
                "--parallel",
                42,
                str(tmp_dir),
                '@"stage name"',
            ]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"put file://{Path(tmp_dir).resolve()}/* '@\"stage name\"' auto_compress=false parallel=42 overwrite=True",
        cursor_class=SnowflakeCursor,
    )


@pytest.mark.parametrize(
    "raw_path,expected_uri",
    [
        ("{}/readme.md", "file://{}/readme.md"),
        ("{}/readme0.md", "file://{}/readme0.md"),
        ("{}/read me.md", "'file://{}/read me.md'"),
        ("{}/read#me.md", "file://{}/read#me.md"),
        ("{}/read*.md", "file://{}/read*.md"),
        ("{}/read_?me.md", "file://{}/read_?me.md"),
        ("{}/read%me.md", "file://{}/read%me.md"),
        ('{}/read"me.md', 'file://{}/read"me.md'),
        ("{}/read'me.md", r"'file://{}/read\'me.md'"),
        ("{}/read\tme.md", r"'file://{}/read\tme.md'"),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_copy_local_to_remote_quoted_path(
    mock_execute, runner, mock_cursor, raw_path, expected_uri
):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir).resolve()
        local_path = raw_path.replace("{}", str(tmp_dir))
        file_uri = expected_uri.replace("{}", str(tmp_dir))

        result = runner.invoke(
            [
                "stage",
                "copy",
                "-c",
                "empty",
                "--overwrite",
                "--parallel",
                42,
                local_path,
                "@stageName",
            ]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"put {file_uri} @stageName auto_compress=false parallel=42 overwrite=True",
        cursor_class=SnowflakeCursor,
    )


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_copy_local_to_remote_star(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            [
                "stage",
                "copy",
                "-c",
                "empty",
                "--overwrite",
                "--parallel",
                42,
                str(tmp_dir) + "/*.py",
                "@stageName",
            ]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"put file://{Path(tmp_dir).resolve()}/*.py @stageName auto_compress=false parallel=42 overwrite=True",
        cursor_class=SnowflakeCursor,
    )


@pytest.mark.parametrize(
    "source, dest",
    [
        ("@snow/stage", "@stage/snow"),
        ("snow://stage", "snow://stage/snow"),
        ("local/path", "other/local/path"),
    ],
)
def test_copy_throws_error_for_same_platform_operation(
    runner, source, dest, os_agnostic_snapshot
):
    result = runner.invoke(["stage", "copy", source, dest])
    assert result.exit_code == 1
    assert result.output == os_agnostic_snapshot


@pytest.mark.parametrize(
    "stage_path, files_on_stage, expected_stage_path, expected_calls",
    [
        (
            "@exe",
            ["a/s2.sql", "a/b/s3.sql", "s1.sql"],
            "@exe",
            [
                "get @exe/a/s2.sql file://{}/a/ parallel=4",
                "get @exe/a/b/s3.sql file://{}/a/b/ parallel=4",
                "get @exe/s1.sql file://{}/ parallel=4",
            ],
        ),
        (
            "@exe/",
            ["a/s2.sql", "a/b/s3.sql", "s1.sql"],
            "@exe/",
            [
                "get @exe/a/s2.sql file://{}/a/ parallel=4",
                "get @exe/a/b/s3.sql file://{}/a/b/ parallel=4",
                "get @exe/s1.sql file://{}/ parallel=4",
            ],
        ),
        (
            "@exe/a",
            ["a/s2.sql", "a/b/s3.sql"],
            "@exe/a",
            [
                "get @exe/a/s2.sql file://{}/ parallel=4",
                "get @exe/a/b/s3.sql file://{}/b/ parallel=4",
            ],
        ),
        (
            "@exe/a/",
            ["a/s2.sql", "a/b/s3.sql"],
            "@exe/a/",
            [
                "get @exe/a/s2.sql file://{}/ parallel=4",
                "get @exe/a/b/s3.sql file://{}/b/ parallel=4",
            ],
        ),
        (
            "@exe/a/b",
            ["a/b/s3.sql"],
            "@exe/a/b",
            [
                "get @exe/a/b/s3.sql file://{}/ parallel=4",
            ],
        ),
        (
            "@exe/a/b/",
            ["a/b/s3.sql"],
            "@exe/a/b/",
            [
                "get @exe/a/b/s3.sql file://{}/ parallel=4",
            ],
        ),
        (
            "@exe/a/b/s3.sql",
            ["a/b/s3.sql"],
            "@exe/a/b/s3.sql",
            [
                "get @exe/a/b/s3.sql file://{}/ parallel=4",
            ],
        ),
        (
            "@exe/s1.sql",
            ["s1.sql"],
            "@exe/s1.sql",
            [
                "get @exe/s1.sql file://{}/ parallel=4",
            ],
        ),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_copy_get_recursive(
    mock_execute,
    mock_cursor,
    temporary_directory,
    stage_path,
    files_on_stage,
    expected_stage_path,
    expected_calls,
):
    mock_execute.return_value = mock_cursor(
        [{"name": f"exe/{file}"} for file in files_on_stage], []
    )

    StageManager().get_recursive(stage_path, Path(temporary_directory))

    ls_call, *copy_calls = mock_execute.mock_calls
    assert ls_call == mock.call(f"ls {expected_stage_path}", cursor_class=DictCursor)
    assert copy_calls == [
        mock.call(c.format(temporary_directory)) for c in expected_calls
    ]


@pytest.mark.parametrize(
    "stage_path, files_on_stage, expected_stage_path, expected_calls",
    [
        (
            "@~",
            ["a/s2.sql", "a/b/s3.sql", "s1.sql"],
            "@~",
            [
                "get '@~/a/s2.sql' file://{}/a/ parallel=4",
                "get '@~/a/b/s3.sql' file://{}/a/b/ parallel=4",
                "get '@~/s1.sql' file://{}/ parallel=4",
            ],
        ),
        (
            "@~/a",
            ["a/s2.sql", "a/b/s3.sql"],
            "@~/a",
            [
                "get '@~/a/s2.sql' file://{}/ parallel=4",
                "get '@~/a/b/s3.sql' file://{}/b/ parallel=4",
            ],
        ),
        (
            "@~/a/b/s3.sql",
            ["a/b/s3.sql"],
            "@~/a/b/s3.sql",
            [
                "get '@~/a/b/s3.sql' file://{}/ parallel=4",
            ],
        ),
        (
            "@~/s1.sql",
            ["s1.sql"],
            "@~/s1.sql",
            [
                "get '@~/s1.sql' file://{}/ parallel=4",
            ],
        ),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_copy_get_recursive_from_user_stage(
    mock_execute,
    mock_cursor,
    temporary_directory,
    stage_path,
    files_on_stage,
    expected_stage_path,
    expected_calls,
):
    mock_execute.return_value = mock_cursor(
        [{"name": file} for file in files_on_stage], []
    )

    StageManager().get_recursive(stage_path, Path(temporary_directory))

    ls_call, *copy_calls = mock_execute.mock_calls
    assert ls_call == mock.call(f"ls '{expected_stage_path}'", cursor_class=DictCursor)
    assert copy_calls == [
        mock.call(c.format(temporary_directory)) for c in expected_calls
    ]


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_create(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["stage", "create", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        "create stage if not exists IDENTIFIER('stageName')"
    )


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_create_quoted(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["stage", "create", "-c", "empty", '"stage name"'])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        """create stage if not exists IDENTIFIER('"stage name"')"""
    )


@mock.patch("snowflake.cli._plugins.object.commands.ObjectManager.execute_query")
def test_stage_drop(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["object", "drop", "stage", "stageName", "-c", "empty"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("drop stage IDENTIFIER('stageName')")


@mock.patch("snowflake.cli._plugins.object.commands.ObjectManager.execute_query")
def test_stage_drop_quoted(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["object", "drop", "stage", '"stage name"', "-c", "empty"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("""drop stage IDENTIFIER('"stage name"')""")


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_remove(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(
        ["stage", "remove", "-c", "empty", "stageName", "my/file/foo.csv"]
    )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("remove @stageName/my/file/foo.csv")


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_remove_quoted(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(
        ["stage", "remove", "-c", "empty", '"stage name"', "my/file/foo.csv"]
    )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("remove '@\"stage name\"/my/file/foo.csv'")


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_print_result_for_put_directory(
    mock_execute, mock_cursor, runner, os_agnostic_snapshot
):
    mock_execute.return_value = mock_cursor(
        rows=[
            ["file1.txt", "file1.txt", 10, 8, "NONE", "NONE", "UPLOADED", ""],
            ["file2.txt", "file2.txt", 10, 8, "NONE", "NONE", "UPLOADED", ""],
            ["file3.txt", "file3.txt", 10, 8, "NONE", "NONE", "UPLOADED", ""],
        ],
        columns=[
            "source",
            "target",
            "source_size",
            "target_size",
            "source_compression",
            "target_compression",
            "status",
            "message",
        ],
    )

    with TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        (tmp_dir_path / "file1.txt").touch()
        (tmp_dir_path / "file2.txt").touch()
        (tmp_dir_path / "file3.txt").touch()
        result = runner.invoke(["stage", "copy", tmp_dir, "@stageName"])

    assert result.exit_code == 0, result.output
    assert result.output == os_agnostic_snapshot


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_print_result_for_get_all_files_from_stage(
    mock_execute, mock_cursor, runner, os_agnostic_snapshot
):
    mock_execute.return_value = mock_cursor(
        rows=[
            ["file1.txt", 10, "DOWNLOADED", ""],
            ["file2.txt", 10, "DOWNLOADED", ""],
            ["file3.txt", 10, "DOWNLOADED", ""],
        ],
        columns=[
            "file",
            "size",
            "status",
            "message",
        ],
    )

    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(["stage", "copy", "@stageName", tmp_dir])

    assert result.exit_code == 0, result.output
    assert result.output == os_agnostic_snapshot


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_print_result_for_get_all_files_from_stage_recursive(
    mock_execute, mock_cursor, runner, os_agnostic_snapshot
):
    columns = ["file", "size", "status", "message"]
    mock_execute.side_effect = [
        mock_cursor(
            [
                {"name": "file1.txt"},
                {"name": "file2.txt"},
                {"name": "file3.txt"},
            ],
            [],
        ),
        mock_cursor([("file1.txt", 10, "DOWNLOADED", "")], columns),
        mock_cursor([("file2.txt", 10, "DOWNLOADED", "")], columns),
        mock_cursor([("file3.txt", 10, "DOWNLOADED", "")], columns),
    ]

    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(["stage", "copy", "@stageName", tmp_dir, "--recursive"])

    assert result.exit_code == 0, result.output
    assert result.output == os_agnostic_snapshot


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_internal_remove(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([("old_role",)], [])
    sm = StageManager()
    sm.remove("stageName", "my/file/foo.csv", "new_role")
    expected = [
        mock.call("select current_role()"),
        mock.call("use role new_role"),
        mock.call("remove @stageName/my/file/foo.csv"),
        mock.call("use role old_role"),
    ]
    assert mock_execute.mock_calls == expected


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_internal_remove_quoted(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([("old_role",)], [])
    sm = StageManager()
    sm.remove('"stage name"', "my/file/foo.csv", "new_role")
    expected = [
        mock.call("select current_role()"),
        mock.call("use role new_role"),
        mock.call("remove '@\"stage name\"/my/file/foo.csv'"),
        mock.call("use role old_role"),
    ]
    assert mock_execute.mock_calls == expected


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_internal_remove_no_role_change(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([("old_role",)], [])
    sm = StageManager()
    sm.remove("stageName", "my/file/foo.csv", "old_role")
    expected = [
        mock.call("select current_role()"),
        mock.call("remove @stageName/my/file/foo.csv"),
    ]
    assert mock_execute.mock_calls == expected


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_internal_put(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([("old_role",)], [])
    with TemporaryDirectory() as tmp_dir:
        sm = StageManager()
        sm.put(Path(tmp_dir).resolve(), "stageName", role="new_role")
        expected = [
            mock.call("select current_role()"),
            mock.call("use role new_role"),
            mock.call(
                f"put file://{Path(tmp_dir).resolve()}/* @stageName auto_compress=false parallel=4 overwrite=False",
                cursor_class=SnowflakeCursor,
            ),
            mock.call("use role old_role"),
        ]
        assert mock_execute.mock_calls == expected


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_internal_put_quoted_stage(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([("old_role",)], [])
    with TemporaryDirectory() as tmp_dir:
        sm = StageManager()
        sm.put(Path(tmp_dir).resolve(), '"stage name"', role="new_role")
        expected = [
            mock.call("select current_role()"),
            mock.call("use role new_role"),
            mock.call(
                f"put file://{Path(tmp_dir).resolve()}/* '@\"stage name\"' auto_compress=false parallel=4 overwrite=False",
                cursor_class=SnowflakeCursor,
            ),
            mock.call("use role old_role"),
        ]
        assert mock_execute.mock_calls == expected


@pytest.mark.parametrize(
    "raw_path,expected_uri",
    [
        ("{}/readme.md", "file://{}/readme.md"),
        ("{}/readme0.md", "file://{}/readme0.md"),
        ("{}/read me.md", "'file://{}/read me.md'"),
        ("{}/read#me.md", "file://{}/read#me.md"),
        ("{}/read*.md", "file://{}/read*.md"),
        ("{}/read_?me.md", "file://{}/read_?me.md"),
        ("{}/read%me.md", "file://{}/read%me.md"),
        ('{}/read"me.md', 'file://{}/read"me.md'),
        ("{}/read'me.md", r"'file://{}/read\'me.md'"),
        ("{}/read\tme.md", r"'file://{}/read\tme.md'"),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_stage_internal_put_quoted_path(
    mock_execute, mock_cursor, raw_path, expected_uri
):
    mock_execute.return_value = mock_cursor([("old_role",)], [])
    with TemporaryDirectory() as tmp_dir:
        sm = StageManager()
        tmp_dir = Path(tmp_dir).resolve()
        src_path = raw_path.replace("{}", str(tmp_dir))
        src_uri = expected_uri.replace("{}", str(tmp_dir))
        sm.put(src_path, "stageName", role="new_role")
        expected = [
            mock.call("select current_role()"),
            mock.call("use role new_role"),
            mock.call(
                f"put {src_uri} @stageName auto_compress=false parallel=4 overwrite=False",
                cursor_class=SnowflakeCursor,
            ),
            mock.call("use role old_role"),
        ]
        assert mock_execute.mock_calls == expected


@pytest.mark.parametrize(
    "stage_path, expected_stage, expected_files",
    [
        ("@exe", "@exe", ["@exe/s1.sql", "@exe/a/S3.sql", "@exe/a/b/s4.sql"]),
        ("exe", "@exe", ["@exe/s1.sql", "@exe/a/S3.sql", "@exe/a/b/s4.sql"]),
        ("exe/", "@exe", ["@exe/s1.sql", "@exe/a/S3.sql", "@exe/a/b/s4.sql"]),
        ("exe/*", "@exe", ["@exe/s1.sql", "@exe/a/S3.sql", "@exe/a/b/s4.sql"]),
        ("exe/*.sql", "@exe", ["@exe/s1.sql", "@exe/a/S3.sql", "@exe/a/b/s4.sql"]),
        ("exe/a", "@exe", ["@exe/a/S3.sql", "@exe/a/b/s4.sql"]),
        ("exe/a/", "@exe", ["@exe/a/S3.sql", "@exe/a/b/s4.sql"]),
        ("exe/a/*", "@exe", ["@exe/a/S3.sql", "@exe/a/b/s4.sql"]),
        ("exe/a/*.sql", "@exe", ["@exe/a/S3.sql", "@exe/a/b/s4.sql"]),
        ("exe/a/b", "@exe", ["@exe/a/b/s4.sql"]),
        ("exe/a/b/", "@exe", ["@exe/a/b/s4.sql"]),
        ("exe/a/b/*", "@exe", ["@exe/a/b/s4.sql"]),
        ("exe/a/b/*.sql", "@exe", ["@exe/a/b/s4.sql"]),
        ("exe/s?.sql", "@exe", ["@exe/s1.sql"]),
        ("exe/s1.sql", "@exe", ["@exe/s1.sql"]),
        (
            "@db.schema.exe",
            "@db.schema.exe",
            [
                "@db.schema.exe/s1.sql",
                "@db.schema.exe/a/S3.sql",
                "@db.schema.exe/a/b/s4.sql",
            ],
        ),
        (
            "db.schema.exe",
            "@db.schema.exe",
            [
                "@db.schema.exe/s1.sql",
                "@db.schema.exe/a/S3.sql",
                "@db.schema.exe/a/b/s4.sql",
            ],
        ),
        ("@db.schema.exe/s1.sql", "@db.schema.exe", ["@db.schema.exe/s1.sql"]),
        ("@db.schema.exe/a/S3.sql", "@db.schema.exe", ["@db.schema.exe/a/S3.sql"]),
        ("@DB.SCHEMA.EXE/s1.sql", "@DB.SCHEMA.EXE", ["@DB.SCHEMA.EXE/s1.sql"]),
        ("@DB.schema.EXE/a/S3.sql", "@DB.schema.EXE", ["@DB.schema.EXE/a/S3.sql"]),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_execute(
    mock_execute,
    mock_cursor,
    runner,
    stage_path,
    expected_stage,
    expected_files,
    os_agnostic_snapshot,
    caplog,
):
    mock_execute.return_value = mock_cursor(
        [
            {"name": "exe/a/S3.sql"},
            {"name": "exe/a/b/s4.sql"},
            {"name": "exe/s1.sql"},
            {"name": "exe/s2"},
        ],
        [],
    )

    result = runner.invoke(["stage", "execute", stage_path])

    assert result.exit_code == 0, result.output
    ls_call, *execute_calls = mock_execute.mock_calls
    assert ls_call == mock.call(f"ls {expected_stage}", cursor_class=DictCursor)
    assert execute_calls == [
        mock.call(f"execute immediate from {p}") for p in expected_files
    ]
    assert result.output == os_agnostic_snapshot
    for expected_file in expected_files:
        assert f"Executing SQL file: {expected_file}" in caplog.messages


@pytest.mark.parametrize(
    "stage_path, expected_files",
    [
        ("@~", ["@~/s1.sql", "@~/a/s3.sql", "@~/a/b/s4.sql"]),
        ("@~/s1.sql", ["@~/s1.sql"]),
        ("@~/a", ["@~/a/s3.sql", "@~/a/b/s4.sql"]),
        ("@~/a/s3.sql", ["@~/a/s3.sql"]),
        ("@~/a/b", ["@~/a/b/s4.sql"]),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_execute_from_user_stage(
    mock_execute,
    mock_cursor,
    runner,
    stage_path,
    expected_files,
    snapshot,
    caplog,
):
    mock_execute.return_value = mock_cursor(
        [
            {"name": "a/s3.sql"},
            {"name": "a/b/s4.sql"},
            {"name": "s1.sql"},
            {"name": "s2"},
        ],
        [],
    )

    result = runner.invoke(["stage", "execute", stage_path])

    assert result.exit_code == 0, result.output
    ls_call, *execute_calls = mock_execute.mock_calls
    assert ls_call == mock.call(f"ls '@~'", cursor_class=DictCursor)
    assert execute_calls == [
        mock.call(f"execute immediate from '{p}'") for p in expected_files
    ]
    assert result.output == snapshot
    for expected_file in expected_files:
        assert f"Executing SQL file: {expected_file}" in caplog.messages


@mock.patch(f"{STAGE_MANAGER}.execute_query")
@mock.patch(f"{STAGE_MANAGER}._bootstrap_snowpark_execution_environment")
@mock.patch(f"{STAGE_MANAGER}.snowpark_session")
@skip_python_3_12
def test_execute_with_variables(
    mock_snowpark_session, mock_bootstrap, mock_execute, mock_cursor, runner
):
    mock_execute.return_value = mock_cursor(
        [{"name": "exe/s1.sql"}, {"name": "exe/s2.py"}], []
    )

    result = runner.invoke(
        [
            "stage",
            "execute",
            "@exe",
            "-D",
            "key1='string value'",
            "-D",
            "key2=1",
            "-D",
            "KEY3=TRUE",
            "-D",
            "key4=NULL",
            "-D",
            "key5='var=value'",
        ]
    )

    assert result.exit_code == 0
    assert mock_execute.mock_calls == [
        mock.call("ls @exe", cursor_class=DictCursor),
        mock.call(
            f"execute immediate from @exe/s1.sql using (key1=>'string value', key2=>1, KEY3=>TRUE, key4=>NULL, key5=>'var=value')"
        ),
    ]
    mock_bootstrap.return_value.assert_called_once_with(
        "@exe/s2.py",
        {
            "key1": "string value",
            "key2": "1",
            "KEY3": "TRUE",
            "key4": "NULL",
            "key5": "var=value",
        },
        session=mock_snowpark_session,
    )


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_execute_raise_invalid_variables_error(
    mock_execute, mock_cursor, runner, os_agnostic_snapshot
):
    mock_execute.return_value = mock_cursor([{"name": "exe/s1.sql"}], [])

    result = runner.invoke(
        [
            "stage",
            "execute",
            "@exe",
            "-D",
            "variable",
        ]
    )

    assert result.exit_code == 1
    assert result.output == os_agnostic_snapshot


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_execute_raise_invalid_file_extension_error(
    mock_execute, mock_cursor, runner, os_agnostic_snapshot
):
    mock_execute.return_value = mock_cursor([{"name": "exe/s1.txt"}], [])

    result = runner.invoke(
        [
            "stage",
            "execute",
            "@exe/s1.txt",
        ]
    )

    assert result.exit_code == 1
    assert result.output == os_agnostic_snapshot


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_execute_not_existing_stage(mock_execute, mock_cursor, runner):
    stage_name = "not_existing_stage"
    mock_execute.side_effect = [
        ProgrammingError(
            f"Stage '{stage_name}' does not exist or not authorized.",
            errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
        )
    ]

    result = runner.invoke(["stage", "execute", stage_name])

    assert result.exit_code == 1
    assert (
        f"002003: 2003: Stage '{stage_name}' does not exist or not authorized."
        in result.output
    )

    assert mock_execute.mock_calls == [
        mock.call(f"ls @{stage_name}", cursor_class=DictCursor)
    ]


@pytest.mark.parametrize(
    "stage_path,expected_message",
    [
        ("exe/*.txt", "No files matched pattern '@exe/*.txt'"),
        ("exe/directory", "No files matched pattern '@exe/directory'"),
        ("exe/some_file.sql", "No files matched pattern '@exe/some_file.sql'"),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_execute_no_files_for_stage_path(
    mock_execute, mock_cursor, runner, stage_path, expected_message
):
    mock_execute.return_value = mock_cursor(
        [
            {"name": "exe/a/s3.sql"},
            {"name": "exe/a/b/s4.sql"},
            {"name": "exe/s1.sql"},
        ],
        [],
    )

    result = runner.invoke(["stage", "execute", stage_path, "--on-error", "continue"])

    assert result.exit_code == 1
    assert expected_message in result.output


@mock.patch(f"{STAGE_MANAGER}.execute_query")
@mock.patch(f"{STAGE_MANAGER}._bootstrap_snowpark_execution_environment")
@mock.patch(f"{STAGE_MANAGER}.snowpark_session")
@skip_python_3_12
def test_execute_stop_on_error(
    mock_snowpark_session, mock_bootstrap, mock_execute, mock_cursor, runner
):
    error_message = "Error"
    mock_execute.side_effect = [
        mock_cursor(
            [
                {"name": "exe/s1.sql"},
                {"name": "exe/p1.py"},
                {"name": "exe/s2.sql"},
                {"name": "exe/p2.py"},
                {"name": "exe/s3.sql"},
            ],
            [],
        ),
        mock_cursor([{"1": 1}], []),
        ProgrammingError(error_message),
    ]

    result = runner.invoke(["stage", "execute", "exe"])
    assert result.exit_code == 1

    assert mock_execute.mock_calls == [
        mock.call("ls @exe", cursor_class=DictCursor),
        mock.call(f"execute immediate from @exe/s1.sql"),
        mock.call(f"execute immediate from @exe/s2.sql"),
    ]
    assert mock_bootstrap.return_value.mock_calls == [
        mock.call("@exe/p1.py", {}, session=mock_snowpark_session),
        mock.call("@exe/p2.py", {}, session=mock_snowpark_session),
    ]
    assert error_message in result.output


@mock.patch(f"{STAGE_MANAGER}.execute_query")
@mock.patch(f"{STAGE_MANAGER}._bootstrap_snowpark_execution_environment")
@mock.patch(f"{STAGE_MANAGER}.snowpark_session")
@skip_python_3_12
def test_execute_continue_on_error(
    mock_snowpark_session,
    mock_bootstrap,
    mock_execute,
    mock_cursor,
    runner,
    os_agnostic_snapshot,
):
    from snowflake.snowpark.exceptions import SnowparkSQLException

    mock_execute.side_effect = [
        mock_cursor(
            [
                {"name": "exe/s1.sql"},
                {"name": "exe/p1.py"},
                {"name": "exe/s2.sql"},
                {"name": "exe/p2.py"},
                {"name": "exe/s3.sql"},
            ],
            [],
        ),
        mock_cursor([{"1": 1}], []),
        ProgrammingError("Error"),
        mock_cursor([{"3": 3}], []),
    ]

    mock_bootstrap.return_value.side_effect = ["ok", SnowparkSQLException("Test error")]

    result = runner.invoke(["stage", "execute", "exe", "--on-error", "continue"])

    assert result.exit_code == 0
    assert result.output == os_agnostic_snapshot
    assert mock_execute.mock_calls == [
        mock.call("ls @exe", cursor_class=DictCursor),
        mock.call(f"execute immediate from @exe/s1.sql"),
        mock.call(f"execute immediate from @exe/s2.sql"),
        mock.call(f"execute immediate from @exe/s3.sql"),
    ]

    assert mock_bootstrap.return_value.mock_calls == [
        mock.call("@exe/p1.py", {}, session=mock_snowpark_session),
        mock.call("@exe/p2.py", {}, session=mock_snowpark_session),
    ]


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize(
    "command, parameters",
    [
        ("list", []),
        ("list", ["--like", "PATTERN"]),
        ("describe", ["NAME"]),
        ("drop", ["NAME"]),
    ],
)
def test_command_aliases(mock_connector, runner, mock_ctx, command, parameters):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["object", command, "stage", *parameters])
    assert result.exit_code == 0, result.output
    result = runner.invoke(["stage", command, *parameters], catch_exceptions=False)
    assert result.exit_code == 0, result.output

    queries = ctx.get_queries()
    assert queries[0] == queries[1]


@pytest.mark.parametrize(
    "files, selected, packages",
    [
        ([], None, []),
        (["my_stage/dir/parallel/requirements.txt"], None, []),
        (
            ["my_stage/dir/files/requirements.txt"],
            "@db.schema.my_stage/dir/files/requirements.txt",
            ["aaa", "bbb"],
        ),
        (
            [
                "my_stage/requirements.txt",
                "my_stage/dir/requirements.txt",
                "my_stage/dir/files/requirements.txt",
            ],
            "@db.schema.my_stage/dir/files/requirements.txt",
            ["aaa", "bbb"],
        ),
        (
            ["my_stage/requirements.txt"],
            "@db.schema.my_stage/requirements.txt",
            ["aaa", "bbb"],
        ),
    ],
)
@pytest.mark.parametrize(
    "input_path", ["@db.schema.my_stage/dir/files", "@db.schema.my_stage/dir/files/"]
)
def test_stage_manager_check_for_requirements_file(
    files, selected, packages, input_path
):
    class _MockGetter:
        def __init__(self):
            self.download_file = None

        def __call__(self, file_on_stage, target_dir):
            self.download_file = file_on_stage
            (target_dir / "requirements.txt").write_text("\n".join(packages))

    get_mock = _MockGetter()
    sm = StageManager()
    with mock.patch.object(
        sm, "_get_files_list_from_stage", lambda parts, pattern: files
    ):
        with mock.patch.object(StageManager, "get", get_mock) as get_mock:
            result = sm._check_for_requirements_file(  # noqa: SLF001
                stage_path=StagePath.from_stage_str(input_path)
            )

    assert result == packages

    assert get_mock.download_file == selected


class RecursiveUploadTester:
    def __init__(self, tmp_dir: str):
        self.calls: list[dict] = []
        self.tmp_dir = Path(tmp_dir)

    def prepare(self, structure: dict):
        def create_structure(root: Path, dir_def: dict):
            for name, content in dir_def.items():
                if isinstance(content, dict):
                    (root / name).mkdir()
                    create_structure(root / name, content)
                else:
                    (root / name).write_text(content)

        create_structure(self.tmp_dir, structure)

    def execute(self, local_path):
        calls = self.calls

        class MockPut(MagicMock):
            def __call__(self, *args, **kwargs):
                if kwargs:
                    calls.append(
                        {
                            "local_path": kwargs["local_path"],
                            "stage_path": kwargs["stage_path"],
                        }
                    )
                return super().__call__(*args, **kwargs)

        class MockTemporaryDirectory(TemporaryDirectory):
            current_name: str

            def __enter__(self):
                MockTemporaryDirectory.current_name = self.name
                return super().__enter__()

        with mock.patch(f"{STAGE_MANAGER}.put", new_callable=MockPut):
            with mock.patch(
                "snowflake.cli._plugins.stage.manager.TemporaryDirectory",
                MockTemporaryDirectory,
            ):
                generator = StageManager().put_recursive(Path(local_path), "stageName")
                list(generator)

        return Path(MockTemporaryDirectory.current_name)


NESTED_STRUCTURE = {
    "dir1": {
        "file1.py": "content1",
        "file1.txt": "content2",
        "dir12": {
            "file121.py": "content3",
            "file122.md": "content4",
        },
    },
    "dir2": {
        "file21": "content3",
        "dir21": {
            "dir211": {
                "dir2111": {
                    "file21111.py": "content4",
                },
            }
        },
    },
    "dir3": {
        "file31": "content5",
        "dir31": {},
        "dir32": {
            "file321": "content6",
        },
    },
    "file4.foo": "content7",
}


@pytest.mark.parametrize("pattern", ["", "**/*", "**"])
def test_recursive_upload(temporary_directory, pattern):
    tester = RecursiveUploadTester(temporary_directory)
    tester.prepare(structure=NESTED_STRUCTURE)
    tmp_created_by_copy = tester.execute(local_path=temporary_directory + "/" + pattern)

    assert tester.calls == [
        # Leaves
        dict(
            local_path=tmp_created_by_copy / "dir2/dir21/dir211/dir2111",
            stage_path=StagePath.from_stage_str("@stageName/dir2/dir21/dir211/dir2111"),
        ),
        dict(
            local_path=tmp_created_by_copy / "dir1/dir12/",
            stage_path=StagePath.from_stage_str("@stageName/dir1/dir12"),
        ),
        dict(
            local_path=tmp_created_by_copy / "dir3/dir32/",
            stage_path=StagePath.from_stage_str("@stageName/dir3/dir32"),
        ),
        # Next level
        dict(
            local_path=tmp_created_by_copy / "dir1/",
            stage_path=StagePath.from_stage_str("@stageName/dir1"),
        ),
        dict(
            local_path=tmp_created_by_copy / "dir3/",
            stage_path=StagePath.from_stage_str("@stageName/dir3"),
        ),
        # Next level
        dict(
            local_path=tmp_created_by_copy / "dir2",
            stage_path=StagePath.from_stage_str("@stageName/dir2"),
        ),
        # Next level
        dict(
            local_path=tmp_created_by_copy,
            stage_path=StagePath.from_stage_str("@stageName"),
        ),
    ]


def test_recursive_upload_with_empty_dir(temporary_directory):
    structure = {}

    tester = RecursiveUploadTester(temporary_directory)
    tester.prepare(structure=structure)
    _ = tester.execute(local_path=temporary_directory)

    assert tester.calls == []


def test_recursive_upload_glob_file_pattern(temporary_directory):
    tester = RecursiveUploadTester(temporary_directory)
    tester.prepare(structure=NESTED_STRUCTURE)
    tmp_created_by_copy = tester.execute(local_path=f"{temporary_directory}/**/*.py")

    assert tester.calls == [
        # Leaves
        dict(
            local_path=tmp_created_by_copy / "dir2/dir21/dir211/dir2111/",
            stage_path=StagePath.from_stage_str("@stageName/dir2/dir21/dir211/dir2111"),
        ),
        dict(
            local_path=tmp_created_by_copy / "dir1/dir12/",
            stage_path=StagePath.from_stage_str("@stageName/dir1/dir12"),
        ),
        # Next level
        dict(
            local_path=tmp_created_by_copy / "dir1/",
            stage_path=StagePath.from_stage_str("@stageName/dir1"),
        ),
    ]


def test_recursive_upload_no_recursive_glob_pattern(temporary_directory):
    tester = RecursiveUploadTester(temporary_directory)
    tester.prepare(structure=NESTED_STRUCTURE)
    tmp_created_by_copy = tester.execute(local_path=f"{temporary_directory}/*.foo")

    assert tester.calls == [
        dict(
            local_path=tmp_created_by_copy,
            stage_path=StagePath.from_stage_str("@stageName"),
        ),
    ]


NESTED_UNBALANCED_STRUCTURE = {
    "dir1": {
        "dir2": {
            "file2.py": "content2",
        },
        "dir3": {
            "dir4": {
                "dir5": {
                    "file5.py": "content5",
                }
            },
        },
    },
}


def test_recursive_unbalanced_tree(temporary_directory):
    """
    SNOW-1966187 - with certain directory structure we were deleting nodes
    before they were processed. This was mostly visible when there was a
    shallow branch and deep branch starting from the same directory.
    """
    tester = RecursiveUploadTester(temporary_directory)
    tester.prepare(structure=NESTED_UNBALANCED_STRUCTURE)
    tester.execute(local_path=temporary_directory + "/")
