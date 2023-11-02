import pytest

from tests_integration.test_utils import row_from_cursor
from tests_integration.snowflake_connector import snowflake_session
from tests_integration.testing_utils.naming_utils import ObjectNameProvider
from tests_integration.snowflake_connector import test_database


@pytest.mark.integration
@pytest.mark.parametrize("object_type", ["table", "warehouse", "schema"])
def test_show(object_type, runner, snowflake_session):
    result = runner.invoke_integration(
        ["object", "show", object_type, "--format", "json"]
    )

    curr = snowflake_session.execute_string(f"show {object_type}s")
    expected = row_from_cursor(curr[-1])

    actual = result.json
    assert len(actual) == len(expected)
    assert actual[0].keys() == expected[0].keys()
    assert actual[0]["name"] == expected[0]["name"]


@pytest.mark.integration
def test_object_table(runner, test_database, snowflake_session):
    object_name = ObjectNameProvider(
        "Test_Object_Table"
    ).create_and_get_next_object_name()
    snowflake_session.execute_string(f"create table {object_name} (some_number NUMBER)")

    result_show = runner.invoke_integration(["object", "show", "table"])
    assert result_show.exit_code == 0

    actual_tables = row_from_cursor(
        snowflake_session.execute_string(f"show tables")[-1]
    )
    assert result_show.json[0].keys() == actual_tables[0].keys()
    assert result_show.json[0]["name"].lower() == object_name.lower()

    result_describe = runner.invoke_integration(
        ["object", "describe", "table", object_name]
    )
    assert result_describe.exit_code == 0
    assert result_describe.json[0]["name"] == "SOME_NUMBER"

    result_drop = runner.invoke_integration(["object", "drop", "table", object_name])
    assert result_drop.exit_code == 0
    assert (
        result_drop.json[0]["status"].lower()
        == f"{object_name.lower()} successfully dropped."
    )
    assert (
        len(row_from_cursor(snowflake_session.execute_string(f"show tables")[-1])) == 0
    )
