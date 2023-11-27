from datetime import datetime
from unittest import mock

import pytest


@pytest.mark.integration
def test_query_parameter(runner, snowflake_session):
    result = runner.invoke_with_connection_json(["sql", "-q", "select pi()"])

    assert result.exit_code == 0
    assert _round_values(result.json[0]) == [{"PI()": 3.14}]


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
        [{"42": 42}]
    ]


def _round_values(results):
    for result in results:
        for k, v in result.items():
            result[k] = round(v, 2)
    return results


def _round_values_for_multi_queries(results):
    return [_round_values(r) for r in results]


@pytest.mark.integration
@pytest.mark.parametrize(
    "input_, query",
    [
        ("select foo;", "select foo;"),
        ("select 1; select foo; select 2;", "select foo;"),
    ],
)
def test_execute_adds_failing_query_to_output(input_, query, runner):

    result = runner.invoke_with_connection(["sql", "-q", input_], catch_exceptions=True)
    assert query in result.output


@pytest.mark.integration
@mock.patch("snowcli.output.printing._get_table")
@mock.patch("snowcli.output.printing.Live")
def test_queries_are_streamed_to_output(
    _, mock_get_table, runner, capsys, test_root_path
):
    runner.invoke_with_connection(
        [
            "sql",
            "-q",
            "select CURRENT_TIME(1); select system$wait(10); select CURRENT_TIME(1);",
        ],
    )

    add_row = mock_get_table().add_row
    results = [p.args[0] for p in add_row.mock_calls]
    assert len(results) == 3
    start, wait, end = results

    assert wait == "waited 10 seconds"

    start_ts = datetime.strptime(start[:-7], "%H:%M:%S")
    end_ts = datetime.strptime(end[:-7], "%H:%M:%S")
    duration = (end_ts - start_ts).total_seconds()
    assert 10.0 < duration <= 11.0
