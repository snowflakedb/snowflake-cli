import json
import pytest

from tests.testing_utils.fixtures import *
from unittest import mock


# Used as a solution to syrupy having some problems with comparing multilines string
class CustomStr(str):
    def __repr__(self):
        return str(self)


MOCK_CONNECTION = {
    "database": "databaseValue",
    "schema": "schemaValue",
    "role": "roleValue",
    "warehouse": "warehouseValue",
}


@pytest.mark.parametrize(
    "cmd,expected",
    [
        ("snow sql", "SNOWCLI.SQL"),
        ("snow warehouse status", "SNOWCLI.WAREHOUSE.STATUS"),
    ],
)
@mock.patch("snowcli.snow_connector.cli_config")
@mock.patch("snowflake.connector.connect")
@mock.patch("snowcli.snow_connector.click")
def test_command_context_is_passed_to_snowflake_connection(
    mock_click, mock_connect, mock_cli_config, runner, cmd, expected, mock_cursor
):
    from snowcli.snow_connector import connect_to_snowflake

    mock_ctx = mock.Mock()
    mock_ctx.command_path = cmd
    mock_click.get_current_context.return_value = mock_ctx
    mock_cli_config.get_connection.return_value = {}

    connect_to_snowflake()

    mock_connect.assert_called_once_with(application=expected)


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_connectivity_error(runner):
    result = runner.invoke_with_config(["sql", "-q", "select 1"])
    assert result.exit_code == 1
    assert "Invalid connection configuration" in result.output
    assert "User is empty" in result.output


@mock.patch("snowflake.connector")
def test_no_output_from_connection(mock_connect, runner):
    funny_text = "what's taters, my precious?"

    def _mock(*args, **kwargs):
        print(funny_text)
        return mock.MagicMock()

    mock_connect.connect = _mock

    result = runner.invoke(["sql", "-q", "select 1"])
    assert funny_text not in result.output
