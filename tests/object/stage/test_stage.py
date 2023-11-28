from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pytest
from snowcli.cli.object.stage.manager import StageManager
from snowflake.connector.cursor import DictCursor

STAGE_MANAGER = "snowcli.cli.object.stage.manager.StageManager"


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_list(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["object", "stage", "list", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("ls @stageName")


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_list_quoted(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["object", "stage", "list", "-c", "empty", '"stage name"'])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("ls '@\"stage name\"'")


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
def test_stage_copy_remote_to_local_quoted(mock_execute, runner, mock_cursor):
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
def test_stage_copy_local_to_remote_quoted(mock_execute, runner, mock_cursor):
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


@mock.patch("snowcli.cli.object.commands.ObjectManager._execute_query")
def test_stage_drop(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke(["object", "drop", "stage", "stageName", "-c", "empty"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("drop stage stageName")


@mock.patch("snowcli.cli.object.commands.ObjectManager._execute_query")
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
def test_stage_internal_remove(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    sm = StageManager()
    sm._remove("stageName", "my/file/foo.csv", "new_role")
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
    sm._remove('"stage name"', "my/file/foo.csv", "new_role")
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
    sm._remove("stageName", "my/file/foo.csv", "old_role")
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
        sm._put(Path(tmp_dir).resolve(), "stageName", "new_role")
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
def test_stage_internal_put_quoted(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    with TemporaryDirectory() as tmp_dir:
        sm = StageManager()
        sm._put(Path(tmp_dir).resolve(), '"stage name"', "new_role")
        expected = [
            mock.call("select current_role()", cursor_class=DictCursor),
            mock.call("use role new_role"),
            mock.call(
                f"put file://{Path(tmp_dir).resolve()} '@\"stage name\"' auto_compress=false parallel=4 overwrite=False"
            ),
            mock.call("use role old_role"),
        ]
        assert mock_execute.mock_calls == expected
