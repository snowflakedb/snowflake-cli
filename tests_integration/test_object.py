import pytest

from tests_integration.test_utils import row_from_cursor
from tests_integration.snowflake_connector import snowflake_session


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
