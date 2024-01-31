import time
from unittest import mock

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


@pytest.mark.integration
def test_multi_queries_where_one_of_them_is_failing(
    runner, snowflake_session, test_root_path, snapshot
):
    result = runner.invoke_with_connection_json(
        ["sql", "-q", f"select 1; select 2; select foo; select 4", "--format", "json"],
        catch_exceptions=True,
    )

    assert result.output == snapshot


@pytest.mark.integration
def test_multi_input_from_stdin(runner, snowflake_session, test_root_path):
    result = runner.invoke_with_connection_json(
        [
            "sql",
            "-i",
        ],
        input="select 1, 2, 3 union select 4, 5, 6; select 42",
    )
    assert result.exit_code == 0
    assert result.json == [
        [{"1": 1, "2": 2, "3": 3}, {"1": 4, "2": 5, "3": 6}],
        [{"42": 42}],
    ]


def _round_values(results):
    for result in results:
        for k, v in result.items():
            result[k] = round(v, 2)
    return results


def _round_values_for_multi_queries(results):
    return [_round_values(r) for r in results]


@pytest.mark.integration
@mock.patch("snowflake.cli.app.printing._get_table")
@mock.patch("snowflake.cli.app.printing.Live")
def test_queries_are_streamed_to_output(
    _, mock_get_table, runner, capsys, test_root_path
):
    log = []

    def _log(
        *args,
    ):
        log.append((time.monotonic(), *args))

    # We mock add_row by a function that logs call time, later we compare call times
    # to make sure those were separated in time
    mock_get_table().add_row = _log

    runner.invoke_with_connection(
        [
            "sql",
            "-q",
            "select 13; select system$wait(10);",
        ],
    )

    assert len(log) == 2
    (time_0, query_0), (time_1, query_1) = log

    assert query_0 == "13"
    assert time_1 - time_0 >= 10.0
    assert "waited 10 seconds" in query_1


@pytest.mark.integration
def test_trailing_comments_queries(runner, snowflake_session, test_root_path):
    trailin_comment_query = "select 1;\n\n-- trailing comment\n"
    result = runner.invoke_with_connection_json(["sql", "-q", trailin_comment_query])
    assert result.exit_code == 0
    assert result.json == [
        [
            {"1": 1},
        ],
    ]
