import pytest

from unittest import mock
from tests_integration.test_utils import extract_data, extract_list_of_data
from tests_integration.snowflake_connector import snowflake_session


@pytest.mark.integration
@mock.patch("snowcli.cli.sql.print_db_cursor")
def test_query_parameter(mock_print, runner, snowflake_session):
    runner.invoke_with_config(["sql", "-q", "select pi()"])

    assert extract_data(mock_print) == [{"PI()": 3.141592654}]


@pytest.mark.integration
@mock.patch("snowcli.cli.sql.print_db_cursor")
def test_multi_queries_from_file(mock_print, runner, snowflake_session, test_root_path):
    runner.invoke_with_config(
        ["sql", "-f", f"{test_root_path}/test_files/sql_multi_queries.sql"]
    )

    assert extract_list_of_data(mock_print) == [
        [{"LN(1)": 0}],
        [{"LN(10)": 2.302585093}],
        [{"LN(100)": 4.605170186}],
    ]
