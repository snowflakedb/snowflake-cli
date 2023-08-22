import pytest
from tests_integration.test_utils import row_from_cursor
from tests_integration.snowflake_connector import snowflake_session


@pytest.mark.integration
def test_warehouse_status_query(runner, snowflake_session):
    result = runner.invoke_integration(["warehouse", "status"])

    expect = snowflake_session.execute_string("show warehouses")[-1]
    assert result.json == row_from_cursor(expect)
