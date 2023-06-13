from pathlib import Path
from unittest import mock


class MockContext:
    database = "some_database"
    schema = "some_schema"
    role = "some_role"
    warehouse = "some_warehouse"


@mock.patch("snowcli.cli.stage.connect_to_snowflake")
def test_default_path_in_get_command(mock_conn, runner):
    mock_conn.return_value.ctx = MockContext()
    result = runner.invoke(["stage", "get", "some_name"])

    assert result.exit_code == 0
    mock_conn.return_value.get_stage.assert_called_once_with(
        database="some_database",
        schema="some_schema",
        role="some_role",
        warehouse="some_warehouse",
        name="some_name",
        path=str(Path(".").absolute()),
    )
