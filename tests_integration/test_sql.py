import pytest


@pytest.mark.integration
def test_query_parameter(runner, snowflake_session):
    result = runner.invoke_with_connection_json(["sql", "-q", "select pi()"])

    assert result.exit_code == 0
    assert _round_values(result.json) == [{"PI()": 3.14}]


@pytest.mark.integration
def test_multi_queries_from_file(runner, snowflake_session, test_root_path):
    result = runner.invoke_with_connection_json(
        [
            "sql",
            "-f",
            f"{test_root_path}/test_data/sql_multi_queries.sql",
        ]
    )

    assert result.exit_code == 0
    assert _round_values_for_multi_queries(result.json) == [
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
