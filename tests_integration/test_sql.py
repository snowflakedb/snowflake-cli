import json
import pytest

from tests_integration.snowflake_connector import snowflake_session


@pytest.mark.integration
def test_query_parameter(runner, snowflake_session):
    result = runner.invoke_with_config_and_integration_connection(
        ["--format", "JSON", "sql", "-q", "select pi()"]
    )

    assert result.exit_code == 0, result.output
    assert _round_values(json.loads(result.output)) == [{"PI()": 3.14}]


@pytest.mark.integration
def test_multi_queries_from_file(runner, snowflake_session, test_root_path):
    result = runner.invoke_with_config_and_integration_connection(
        [
            "--format",
            "JSON",
            "sql",
            "-f",
            f"{test_root_path}/test_files/sql_multi_queries.sql",
        ]
    )

    assert result.exit_code == 0, result.output
    assert _round_values_for_multi_queries(json.loads(result.output)) == [
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
