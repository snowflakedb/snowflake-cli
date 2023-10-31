from tests.testing_utils.fixtures import *


@mock.patch("snowflake.connector")
@pytest.mark.parametrize("object_name", ["compute pool", "database", "streamlit"])
def test_show_warehouse(mock_connector, object_name, mock_cursor, runner, snapshot):
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
    result = runner.invoke(["object", "show", object_name], catch_exceptions=False)
    assert result.exit_code == 0, result.output
    assert result.output == snapshot
