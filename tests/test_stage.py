from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from tests.testing_utils.fixtures import *

STAGE_MANAGER = "snowcli.cli.stage.manager.StageManager"


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_list(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke_with_config(["stage", "list", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("ls @stageName")


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_get(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke_with_config(
            ["stage", "get", "-c", "empty", "stageName", str(tmp_dir)]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"get @stageName file://{Path(tmp_dir).resolve()}/"
    )


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_get_default_path(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke_with_config(["stage", "get", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f'get @stageName file://{Path(".").resolve()}/'
    )


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_put(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke_with_config(
            [
                "stage",
                "put",
                "-c",
                "empty",
                "--overwrite",
                "--parallel",
                42,
                str(tmp_dir),
                "stageName",
            ]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"put file://{Path(tmp_dir).resolve()}/* @stageName auto_compress=false parallel=42 overwrite=True"
    )


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_put_star(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke_with_config(
            [
                "stage",
                "put",
                "-c",
                "empty",
                "--overwrite",
                "--parallel",
                42,
                str(tmp_dir) + "/*.py",
                "stageName",
            ]
        )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        f"put file://{Path(tmp_dir).resolve()}/*.py @stageName auto_compress=false parallel=42 overwrite=True"
    )


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_create(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke_with_config(["stage", "create", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("create stage if not exists stageName")


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_drop(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke_with_config(["stage", "drop", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("drop stage stageName")


@mock.patch(f"{STAGE_MANAGER}._execute_query")
def test_stage_remove(mock_execute, runner, mock_cursor):
    mock_execute.return_value = mock_cursor(["row"], [])
    result = runner.invoke_with_config(
        ["stage", "remove", "-c", "empty", "stageName", "my/file/foo.csv"]
    )
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("remove @stageName/my/file/foo.csv")
