from tests.testing_utils.fixtures import *


# Used as a solution to syrupy having some problems with comparing multilines string
class CustomStr(str):
    def __repr__(self):
        return str(self)


MOCK_CONNECTION = {
    "database": "databaseValue",
    "schema": "schemaValue",
    "role": "roleValue",
    "show": "warehouseValue",
}


@pytest.mark.parametrize(
    "cmd,expected",
    [
        ("snow sql", "SNOWCLI.SQL"),
        ("snow show warehouses", "SNOWCLI.SHOW.WAREHOUSES"),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.app.snow_connector.click")
def test_command_context_is_passed_to_snowflake_connection(
    mock_click, mock_connect, cmd, expected
):
    from snowflake.cli.app.snow_connector import connect_to_snowflake

    mock_ctx = mock.Mock()
    mock_ctx.command_path = cmd
    mock_click.get_current_context.return_value = mock_ctx

    connect_to_snowflake()

    mock_connect.assert_called_once_with(
        application=expected,
        database="db_for_test",
        schema="test_public",
        role="test_role",
        warehouse="xs",
        password="dummy_password",
    )


@mock.patch.dict(os.environ, {}, clear=True)
def test_returns_nice_error_in_case_of_connectivity_error(runner):
    result = runner.invoke(["sql", "-q", "select 1"])
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
