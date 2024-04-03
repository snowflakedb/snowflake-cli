from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pytest
from snowflake.cli.plugins.object.stage.manager import StageManager
from snowflake.connector.cursor import DictCursor

STAGE_MANAGER = "snowflake.cli.plugins.object.stage.manager.StageManager"


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_list(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["object", "stage", "list", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("ls @stageName", cursor_class=DictCursor)


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_list_pattern(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(
        ["object", "stage", "list", "-c", "empty", "--pattern", "REGEX", "stageName"]
    )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        "ls @stageName pattern = 'REGEX'", cursor_class=DictCursor
    )


def test_stage_list_pattern_error(runner):
    result = runner.invoke(
        ["object", "stage", "list", "--pattern", "REGEX without escaped '", "stageName"]
    )
    assert result.exit_code == 1, result.output
    assert "Error" in result.output
    assert 'All "\'" characters in PATTERN must be escaped: "\\\'"' in result.output


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_list_quoted(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["object", "stage", "list", "-c", "empty", '"stage name"'])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        "ls '@\"stage name\"'", cursor_class=DictCursor
    )


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_copy_remote_to_local(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            ["object", "stage", "copy", "-c", "empty", "@stageName", str(tmp_dir)]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"get @stageName file://{Path(tmp_dir).resolve()}/ parallel=4"
    )


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_copy_remote_to_local_quoted_stage(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            ["object", "stage", "copy", "-c", "empty", '@"stage name"', str(tmp_dir)]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"get '@\"stage name\"' file://{Path(tmp_dir).resolve()}/ parallel=4"
    )


@mock.patch(f"{STAGE_MANAGER}._execute_query")
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
                "object",
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
@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_copy_remote_to_local_quoted_uri(
    mock_execute, runner, mock_cursor, raw_path, expected_uri
):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir).resolve()
        local_path = raw_path.replace("{}", str(tmp_dir))
        file_uri = expected_uri.replace("{}", str(tmp_dir))
        result = runner.invoke(
            ["object", "stage", "copy", "-c", "empty", "@stageName", local_path]
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
@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_copy_remote_to_local_quoted_uri_recursive(
    mock_execute, runner, mock_cursor, raw_path, expected_uri
):
    mock_execute.side_effect = [
        mock_cursor([{"name": "stageName/file"}], []),
        mock_cursor([(raw_path)], ["file"]),
    ]
    with TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir).resolve()
        local_path = raw_path.replace("{}", str(tmp_dir))
        file_uri = expected_uri.replace("{}", str(tmp_dir))
        result = runner.invoke(
            [
                "object",
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
        mock.call(f"get @stageName/file {file_uri} parallel=4"),
    ]


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_copy_local_to_remote(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            [
                "object",
                "stage",
                "copy",
                "-c",
                "empty",
                "--overwrite",
                "--parallel",
                42,
                str(tmp_dir),
                "@stageName",
            ]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"put file://{Path(tmp_dir).resolve()}/* @stageName auto_compress=false parallel=42 overwrite=True"
    )


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_copy_local_to_remote_quoted_stage(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            [
                "object",
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
        f"put file://{Path(tmp_dir).resolve()}/* '@\"stage name\"' auto_compress=false parallel=42 overwrite=True"
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
@mock.patch(f"{STAGE_MANAGER}._execute_query")
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
                "object",
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
        f"put {file_uri} @stageName auto_compress=false parallel=42 overwrite=True"
    )


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_copy_local_to_remote_star(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke(
            [
                "object",
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
        f"put file://{Path(tmp_dir).resolve()}/*.py @stageName auto_compress=false parallel=42 overwrite=True"
    )


@pytest.mark.parametrize(
    "source, dest",
    [
        ("@snow/stage", "@stage/snow"),
        ("snow://stage", "snow://stage/snow"),
        ("local/path", "other/local/path"),
    ],
)
def test_copy_throws_error_for_same_platform_operation(runner, source, dest, snapshot):
    result = runner.invoke(["object", "stage", "copy", source, dest])
    assert result.exit_code == 1
    assert result.output == snapshot


@pytest.mark.parametrize(
    "stage_path, files_on_stage, expected_calls",
    [
        (
            "@exe",
            ["a/s2.sql", "a/b/s3.sql", "s1.sql"],
            [
                "get @exe/a/s2.sql file://{}/a/ parallel=4",
                "get @exe/a/b/s3.sql file://{}/a/b/ parallel=4",
                "get @exe/s1.sql file://{}/ parallel=4",
            ],
        ),
        (
            "@exe/",
            ["a/s2.sql", "a/b/s3.sql", "s1.sql"],
            [
                "get @exe/a/s2.sql file://{}/a/ parallel=4",
                "get @exe/a/b/s3.sql file://{}/a/b/ parallel=4",
                "get @exe/s1.sql file://{}/ parallel=4",
            ],
        ),
        (
            "snow://exe",
            ["a/s2.sql", "a/b/s3.sql", "s1.sql"],
            [
                "get @exe/a/s2.sql file://{}/a/ parallel=4",
                "get @exe/a/b/s3.sql file://{}/a/b/ parallel=4",
                "get @exe/s1.sql file://{}/ parallel=4",
            ],
        ),
        (
            "@exe/a",
            ["a/s2.sql", "a/b/s3.sql"],
            [
                "get @exe/a/s2.sql file://{}/ parallel=4",
                "get @exe/a/b/s3.sql file://{}/b/ parallel=4",
            ],
        ),
        (
            "@exe/a/",
            ["a/s2.sql", "a/b/s3.sql"],
            [
                "get @exe/a/s2.sql file://{}/ parallel=4",
                "get @exe/a/b/s3.sql file://{}/b/ parallel=4",
            ],
        ),
        (
            "@exe/a/b",
            ["a/b/s3.sql"],
            [
                "get @exe/a/b/s3.sql file://{}/ parallel=4",
            ],
        ),
        (
            "@exe/a/b/",
            ["a/b/s3.sql"],
            [
                "get @exe/a/b/s3.sql file://{}/ parallel=4",
            ],
        ),
        (
            "@exe/a/b/s3.sql",
            ["a/b/s3.sql"],
            [
                "get @exe/a/b/s3.sql file://{}/ parallel=4",
            ],
        ),
        (
            "@exe/s1.sql",
            ["s1.sql"],
            [
                "get @exe/s1.sql file://{}/ parallel=4",
            ],
        ),
    ],
)
@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_copy_get_recursive(
    mock_execute, mock_cursor, temp_dir, stage_path, files_on_stage, expected_calls
):
    mock_execute.return_value = mock_cursor(
        [{"name": f"exe/{file}"} for file in files_on_stage], []
    )

    StageManager().get_recursive(stage_path, Path(temp_dir))

    ls_call, *copy_calls = mock_execute.mock_calls
    assert ls_call == mock.call(f"ls {stage_path}", cursor_class=DictCursor)
    assert copy_calls == [mock.call(c.format(temp_dir)) for c in expected_calls]


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_create(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["object", "stage", "create", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("create stage if not exists stageName")


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_create_quoted(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["object", "stage", "create", "-c", "empty", '"stage name"'])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with('create stage if not exists "stage name"')


@mock.patch("snowflake.cli.plugins.object.commands.ObjectManager._execute_query")
def test_stage_drop(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["object", "drop", "stage", "stageName", "-c", "empty"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("drop stage stageName")


@mock.patch("snowflake.cli.plugins.object.commands.ObjectManager._execute_query")
def test_stage_drop_quoted(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["object", "drop", "stage", '"stage name"', "-c", "empty"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with('drop stage "stage name"')


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_remove(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(
        ["object", "stage", "remove", "-c", "empty", "stageName", "my/file/foo.csv"]
    )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("remove @stageName/my/file/foo.csv")


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_remove_quoted(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(
        ["object", "stage", "remove", "-c", "empty", '"stage name"', "my/file/foo.csv"]
    )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("remove '@\"stage name\"/my/file/foo.csv'")


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_print_result_for_put_directory(
    mock_execute, mock_cursor, runner, snapshot
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
        result = runner.invoke(["object", "stage", "copy", tmp_dir, "@stageName"])

    assert result.exit_code == 0, result.output
    assert result.output == snapshot


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_print_result_for_get_all_files_from_stage(
    mock_execute, mock_cursor, runner, snapshot
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
        result = runner.invoke(["object", "stage", "copy", "@stageName", tmp_dir])

    assert result.exit_code == 0, result.output
    assert result.output == snapshot


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_print_result_for_get_all_files_from_stage_recursive(
    mock_execute, mock_cursor, runner, snapshot
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
        result = runner.invoke(
            ["object", "stage", "copy", "@stageName", tmp_dir, "--recursive"]
        )

    assert result.exit_code == 0, result.output
    assert result.output == snapshot


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_internal_remove(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    sm = StageManager()
    sm.remove("stageName", "my/file/foo.csv", "new_role")
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role new_role"),
        mock.call("remove @stageName/my/file/foo.csv"),
        mock.call("use role old_role"),
    ]
    assert mock_execute.mock_calls == expected


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_internal_remove_quoted(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    sm = StageManager()
    sm.remove('"stage name"', "my/file/foo.csv", "new_role")
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role new_role"),
        mock.call("remove '@\"stage name\"/my/file/foo.csv'"),
        mock.call("use role old_role"),
    ]
    assert mock_execute.mock_calls == expected


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_internal_remove_no_role_change(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    sm = StageManager()
    sm.remove("stageName", "my/file/foo.csv", "old_role")
    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("remove @stageName/my/file/foo.csv"),
    ]
    assert mock_execute.mock_calls == expected


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_internal_put(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    with TemporaryDirectory() as tmp_dir:
        sm = StageManager()
        sm.put(Path(tmp_dir).resolve(), "stageName", role="new_role")
        expected = [
            mock.call("select current_role()", cursor_class=DictCursor),
            mock.call("use role new_role"),
            mock.call(
                f"put file://{Path(tmp_dir).resolve()} @stageName auto_compress=false parallel=4 overwrite=False"
            ),
            mock.call("use role old_role"),
        ]
        assert mock_execute.mock_calls == expected


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_internal_put_quoted_stage(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    with TemporaryDirectory() as tmp_dir:
        sm = StageManager()
        sm.put(Path(tmp_dir).resolve(), '"stage name"', role="new_role")
        expected = [
            mock.call("select current_role()", cursor_class=DictCursor),
            mock.call("use role new_role"),
            mock.call(
                f"put file://{Path(tmp_dir).resolve()} '@\"stage name\"' auto_compress=false parallel=4 overwrite=False"
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
@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_internal_put_quoted_path(
    mock_execute, mock_cursor, raw_path, expected_uri
):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    with TemporaryDirectory() as tmp_dir:
        sm = StageManager()
        tmp_dir = Path(tmp_dir).resolve()
        src_path = raw_path.replace("{}", str(tmp_dir))
        src_uri = expected_uri.replace("{}", str(tmp_dir))
        sm.put(src_path, "stageName", role="new_role")
        expected = [
            mock.call("select current_role()", cursor_class=DictCursor),
            mock.call("use role new_role"),
            mock.call(
                f"put {src_uri} @stageName auto_compress=false parallel=4 overwrite=False"
            ),
            mock.call("use role old_role"),
        ]
        assert mock_execute.mock_calls == expected
