import pytest

from unittest import mock
from tests_integration.test_utils import row_from_mock, rows_from_mock
from tests_integration.snowflake_connector import snowflake_session


@pytest.mark.integration
@mock.patch("snowcli.cli.sql.print_db_cursor")
def test_query_parameter(mock_print, runner, snowflake_session):
    result = runner.invoke_with_config_and_integration_connection(
        ["sql", "-q", "select pi()"]
    )

    assert result.exit_code == 0, result.output
    assert _round_values(row_from_mock(mock_print)) == [{"PI()": 3.14}]


@pytest.mark.integration
@mock.patch("snowcli.cli.sql.print_db_cursor")
def test_multi_queries_from_file(mock_print, runner, snowflake_session, test_root_path):
    result = runner.invoke_with_config_and_integration_connection(
        ["sql", "-f", f"{test_root_path}/test_files/sql_multi_queries.sql"]
    )

    assert result.exit_code == 0, result.output
    assert _round_values_for_multi_queries(rows_from_mock(mock_print)) == [
        [{"LN(1)": 0.00}],
        [{"LN(10)": 2.30}],
        [{"LN(100)": 4.61}],
    ]


def _round_values(results):
    for result in results:
        for k, v in result.items():
            result[k] = round(v, 2)
    return results


def _round_values_for_multi_queries(results):
    return [_round_values(r) for r in results]
