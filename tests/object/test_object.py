from tests.testing_utils.fixtures import *


@mock.patch("snowflake.connector")
@pytest.mark.parametrize(
    "object_type", ["warehouse", "compute pool", "database", "streamlit"]
)
def test_show(mock_connector, object_type, mock_cursor, runner, snapshot):
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
    result = runner.invoke(["object", "show", object_type], catch_exceptions=False)
    assert result.exit_code == 0, result.output
    assert result.output == snapshot


@mock.patch("snowflake.connector")
@pytest.mark.parametrize(
    "object_type, object_name ", [("warehouse", "xsmall"), ("table", "example")]
)
def test_describe(
    mock_connector, object_type, object_name, mock_cursor, runner, snapshot
):
    mock_connector.connect.return_value.execute_string.return_value = (
        None,
        mock_cursor(
            rows=[("ID", "NUMBER(38,0", "COLUMN"), ("NAME", "VARCHAR(100", "COLUMN")],
            columns=["name", "type", "kind"],
        ),
    )
    result = runner.invoke(["object", "describe", object_type, object_name])
    assert result.exit_code == 0, result.output
    assert result.output == snapshot


@mock.patch("snowflake.connector")
@pytest.mark.parametrize(
    "object_type, object_name",
    [("table", "example"), ("database", "important_prod_db"), ("warehouse", "xsmall")],
)
def test_drop(mock_connector, object_type, object_name, mock_cursor, runner, snapshot):
    mock_connector.connect.return_value.execute_string.return_value = (
        None,
        mock_cursor(rows=[f"{object_name} successfully dropped."], columns=["status"]),
    )

    result = runner.invoke(["object", "drop", object_type, object_name])
    assert result.exit_code == 0, result.output
    assert result.output == snapshot
