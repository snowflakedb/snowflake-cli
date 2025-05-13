import pytest
from typing import Optional


@pytest.fixture
def existing_query_id(runner):
    result = runner.invoke_with_connection_json(["sql", "-q", "select 15;>"])
    assert result.exit_code == 0, result.output
    yield result.json[0]["scheduled query ID"]


@pytest.mark.integration
def test_queries(runner):
    query = """select 1;
    select 3;
    select 13;
    !queries;
    select 4;"""
    result = runner.invoke_with_connection(["sql", "-q", query])
    assert result.exit_code == 0, result.output
    assert result.output.count("select 13") == 2
    assert result.output.count("select 3") == 2
    assert result.output.count("select 4") == 1
    assert result.output.count("SUCCEEDED") == 3
    for header in ["QUERY ID", "SQL TEXT", "STATUS", "DURATION_MS"]:
        assert header in result.output


@pytest.mark.integration
def test_queries_help(runner, snapshot):
    result = runner.invoke_with_connection(["sql", "-q", "!queries help;"])
    assert result.exit_code == 0, result.output
    assert result.output == snapshot


@pytest.mark.integration
def test_result(runner, existing_query_id):
    result = runner.invoke_with_connection_json(
        ["sql", "-q", f"!result {existing_query_id}"]
    )
    assert result.exit_code == 0, result.output
    assert result.json == [{"15": 15}]


@pytest.mark.integration
def test_abort(runner, existing_query_id):
    result = runner.invoke_with_connection_json(
        ["sql", "-q", f"!abort {existing_query_id}"]
    )
    assert result.exit_code == 0, result.output
    assert result.json == [
        {
            f"SYSTEM$CANCEL_QUERY('{existing_query_id.upper()}')": "Identified SQL statement is not currently executing.",
        },
    ]
