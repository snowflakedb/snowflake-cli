import re
from datetime import datetime
from io import StringIO

import pytest
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import escape_like_pattern
from snowflake.connector.util_text import split_statements


def test_providing_to_time_and_refresh_causes_error(runner):

    result = runner.invoke(
        [
            "logs",
            "table",
            "test_table",
            "--to",
            '"2022-02-02 02:02:02"',
            "--refresh",
            "5",
        ]
    )
    assert result.exit_code == 1
    assert "You cannot set both --refresh and --to parameters" in result.output


def test_providing_to_time_earlier_than_from_time_causes_error(runner):

    result = runner.invoke(
        [
            "logs",
            "table",
            "test_table",
            "--from",
            "2022-02-02 02:02:02",
            "--to",
            "2022-02-01 02:02:02",
        ]
    )
    assert result.exit_code == 1
    assert "From_time cannot be later than to_time" in result.output


@pytest.mark.parametrize("parameter", ["--from", "--to"])
@pytest.mark.parametrize(
    "time_string",
    [
        "2024-11-03 12:00:00 UTC",
        "2024.11.03 12 00",
        "About one hour ago",
        "92348573948753202",
    ],
)
def test_providing_time_in_incorrect_format_causes_error(
    time_string, parameter, runner, snapshot
):
    result = runner.invoke(["logs", "compute_pool", "foo", parameter, time_string])

    assert result.exit_code == 1
    assert result.output == snapshot


@pytest.mark.parametrize("table", ["foo", "bar", None])
def test_correct_query_is_constructed(mock_connect, mock_ctx, runner, snapshot, table):
    ctx = mock_ctx()
    mock_connect.return_value = ctx

    args = [
        "logs",
        "compute_pool",
        "bar",
        "--from",
        "2022-02-02 02:02:02",
        "--to",
        "2022-02-03 02:02:02",
    ]

    if table:
        args.extend(["--table", table])

    _ = runner.invoke(args)

    queries = ctx.get_queries()
    assert len(queries) == 1
    assert queries[0] == snapshot


def test_if_incorrect_log_level_causes_error(runner, snapshot):
    result = runner.invoke(
        [
            "logs",
            "table",
            "test_table",
            "--log-level",
            "NOTALEVEL",
        ]
    )
    assert result.exit_code == 1
    assert result.output == snapshot


@pytest.mark.parametrize("partial_match", [True, False])
@pytest.mark.parametrize("object_name", ["test_obj", "MyObject"])
def test_partial_match_query_construction(
    mock_connect, mock_ctx, runner, snapshot, partial_match, object_name
):
    """Test that SQL queries are properly constructed for both exact and partial matching"""
    ctx = mock_ctx()
    mock_connect.return_value = ctx

    args = [
        "logs",
        "table",
        object_name,
        "--from",
        "2022-02-02 02:02:02",
        "--to",
        "2022-02-03 02:02:02",
    ]

    if partial_match:
        args.append("--partial")

    _ = runner.invoke(args)

    queries = ctx.get_queries()
    assert len(queries) == 1
    assert queries[0] == snapshot


@pytest.mark.parametrize("object_name", ["test_obj_with_underscore", '"test%obj"'])
def test_partial_match_with_like_wildcards(
    mock_connect, mock_ctx, runner, snapshot, object_name
):
    """Test that SQL LIKE wildcard characters in object names are properly escaped when using partial matching"""
    ctx = mock_ctx()
    mock_connect.return_value = ctx

    args = [
        "logs",
        "table",
        object_name,
        "--partial",
    ]

    _ = runner.invoke(args)

    queries = ctx.get_queries()
    assert len(queries) == 1
    assert queries[0] == snapshot


def _visible_literal_value(value: str) -> str:
    """String Snowflake reads after odd backslash runs gain one backslash.

    Even runs before a quote stay unchanged: quote-doubling already keeps
    them inside one literal, and doubling the run would name a different
    object. Walks the characters so this expectation is not the SQL regex.
    """
    out: list[str] = []
    index = 0
    while index < len(value):
        if value[index] != "\\":
            out.append(value[index])
            index += 1
            continue
        end = index
        while end < len(value) and value[end] == "\\":
            end += 1
        run = value[index:end]
        if end < len(value) and value[end] == "'":
            if len(run) % 2 == 1:
                run += "\\"
            out.append(run + "'")
            index = end + 1
        elif end == len(value) and len(run) % 2 == 1:
            out.append(run + "\\")
            index = end
        else:
            out.append(run)
            index = end
    return "".join(out)


def _condition_literal_value(query: str) -> str:
    match = re.search(r"object_name (?:ILIKE|=) ('(?:''|[^'])*')", query)
    assert match, query
    return match.group(1)[1:-1].replace("''", "'")


@pytest.mark.parametrize("partial_match", [False, True])
@pytest.mark.parametrize(
    "object_name",
    [
        # One backslash before a quote. Quote-doubling alone leaves \'' ,
        # and split_statements ends the literal on the second quote.
        '"x\\\';DROP TABLE t;--"',
        # Same breakout, with LIKE metacharacters that --partial must still escape.
        '"pct%_x\\\';select 1;--"',
        # Three backslashes: the run is odd, so it needs one more, not a doubling.
        '"x\\\\\\\';DROP TABLE t;--"',
        # Two backslashes: already one statement after quote-doubling. The
        # query must keep both, or it looks up a different object.
        '"x\\\\\';DROP TABLE t;--"',
        '"pct%_x\\\\\';select 1;--"',
        # Quote with no backslash, and a wildcard name on the exact-match path.
        '"foo\'bar; DROP TABLE t;--"',
        '"test%_obj"',
        '"café"',
        'DB.SC."a\\\\\'b"',
    ],
)
def test_object_name_backslash_quote_stays_in_one_statement(
    mock_connect, mock_cursor, mock_ctx, runner, partial_match, object_name
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[(datetime(2024, 1, 1), "DB", "SC", "OBJ", "INFO", "msg")],
            columns=[
                "timestamp",
                "database_name",
                "schema_name",
                "object_name",
                "log_level",
                "log_message",
            ],
        )
    )
    mock_connect.return_value = ctx

    args = ["logs", "table", object_name]
    if partial_match:
        args.append("--partial")

    result = runner.invoke(args)
    assert result.exit_code == 0, result.output

    queries = ctx.get_queries()
    assert len(queries) == 1
    query = queries[0]

    statements = [text for text, _ in split_statements(StringIO(query)) if text.strip()]
    assert len(statements) == 1

    name = str(FQN.from_string(object_name))
    if partial_match:
        source = "%" + escape_like_pattern(name, escape_sequence="\\") + "%"
    else:
        source = name
    assert _condition_literal_value(query) == _visible_literal_value(source)
