from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock


@mock.patch("snowcli.snow_connector.connect_to_snowflake")
@mock.patch("snowcli.cli.stage.StageManager._execute_query")
def test_stage_list(mock_execute, mock_conn, runner):
    result = runner.invoke_with_config(["stage", "list", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_conn.assert_called_once_with(connection_name="empty")
    mock_execute.assert_called_once_with("ls @stageName")


@mock.patch("snowcli.snow_connector.connect_to_snowflake")
@mock.patch("snowcli.cli.stage.StageManager._execute_query")
def test_stage_get(mock_execute, mock_conn, runner):
    with TemporaryDirectory() as tmp_dir:
        result = runner.invoke_with_config(
            ["stage", "get", "-c", "empty", "stageName", str(tmp_dir)]
        )
    assert result.exit_code == 0, result.output
    mock_conn.assert_called_once_with(connection_name="empty")
    mock_execute.assert_called_once_with(
        f"get @stageName file://{Path(tmp_dir).resolve()}/"
    )


@mock.patch("snowcli.snow_connector.connect_to_snowflake")
@mock.patch("snowcli.cli.stage.StageManager._execute_query")
def test_stage_get_default_path(mock_execute, mock_conn, runner):
    result = runner.invoke_with_config(["stage", "get", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_conn.assert_called_once_with(connection_name="empty")
    mock_execute.assert_called_once_with(
        f'get @stageName file://{Path(".").resolve()}/'
    )


@mock.patch("snowcli.snow_connector.connect_to_snowflake")
@mock.patch("snowcli.cli.stage.StageManager._execute_query")
def test_stage_put(mock_execute, mock_conn, runner):
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
    mock_conn.assert_called_once_with(connection_name="empty")
    mock_execute.assert_called_once_with(
        f"put file://{Path(tmp_dir).resolve()}/* @stageName auto_compress=false parallel=42 overwrite=True"
    )


@mock.patch("snowcli.snow_connector.connect_to_snowflake")
@mock.patch("snowcli.cli.stage.StageManager._execute_query")
def test_stage_create(mock_execute, mock_conn, runner):
    result = runner.invoke_with_config(["stage", "create", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_conn.assert_called_once_with(connection_name="empty")
    mock_execute.assert_called_once_with("create stage if not exists stageName")


@mock.patch("snowcli.snow_connector.connect_to_snowflake")
@mock.patch("snowcli.cli.stage.StageManager._execute_query")
def test_stage_drop(mock_execute, mock_conn, runner):
    result = runner.invoke_with_config(["stage", "drop", "-c", "empty", "stageName"])
    assert result.exit_code == 0, result.output
    mock_conn.assert_called_once_with(connection_name="empty")
    mock_execute.assert_called_once_with("drop stage stageName")


@mock.patch("snowcli.snow_connector.connect_to_snowflake")
@mock.patch("snowcli.cli.stage.StageManager._execute_query")
def test_stage_remove(mock_execute, mock_conn, runner):
    result = runner.invoke_with_config(
        ["stage", "remove", "-c", "empty", "stageName", "my/file/foo.csv"]
    )
    assert result.exit_code == 0, result.output
    mock_conn.assert_called_once_with(connection_name="empty")
    mock_execute.assert_called_once_with("remove @stageName/my/file/foo.csv")
