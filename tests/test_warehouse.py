from tests.testing_utils.fixtures import *


@mock.patch("snowflake.connector")
def test_show_warehouses(mock_connector, mock_cursor, runner, snapshot):
    mock_connector.connect.return_value.execute_string.return_value = (
        None,
        mock_cursor(
            rows=[
                (
                    "foo",
                    "suspended",
                ),
                ("bar", "running"),
            ],
            columns=["name", "state"],
        ),
    )
    result = runner.invoke(["warehouse", "status"], catch_exceptions=False)
    assert result.exit_code == 0, result.output
    assert result.output == snapshot
