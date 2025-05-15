import pytest
from typing import Optional
from os import getenv


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
@pytest.mark.parametrize("use_iso_format", [True, False])
def test_queries_time_filters(runner, existing_query_id, use_iso_format):
    import datetime

    now = datetime.datetime.now()
    an_hour_ago = now - datetime.timedelta(hours=1)
    two_hours_ago = now - datetime.timedelta(hours=2)
    in_five_minutes = now + datetime.timedelta(minutes=5)

    # wait for the query to execute
    result = runner.invoke_with_connection(
        ["sql", "-q", f"!result {existing_query_id}"]
    )
    assert result.exit_code == 0, result.output

    def _format_filter_no_prefix(time: datetime.datetime):
        if use_iso_format:
            return f"_date={time.isoformat()}"
        return f"={int(time.timestamp() * 1000)}"

    def _query(start: Optional[datetime.datetime], end: Optional[datetime.datetime]):
        user = getenv("SNOWFLAKE_CONNECTIONS_INTEGRATION_USER", "")
        result = f"!queries user={user}"
        if start:
            result += f" start{_format_filter_no_prefix(start)}"
        if end:
            result += f" end{_format_filter_no_prefix(end)}"
        return result

    for start, end, query_expected in [
        (in_five_minutes, None, False),
        (two_hours_ago, an_hour_ago, False),
        (an_hour_ago, in_five_minutes, True),
        (an_hour_ago, None, True),
    ]:
        result = runner.invoke_with_connection_json(["sql", "-q", _query(start, end)])
        assert result.exit_code == 0, result.output
        query_ids = [row["QUERY ID"] for row in result.json]
        assert query_expected == (existing_query_id in query_ids)


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
