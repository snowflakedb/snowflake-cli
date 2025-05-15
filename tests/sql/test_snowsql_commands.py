from unittest import mock

import pytest
from snowflake.cli._plugins.sql.snowsql_commands import (
    AbortCommand,
    CompileCommandResult,
    QueriesCommand,
    ResultCommand,
    SnowSQLCommand,
    compile_snowsql_command,
)

_FAKE_QID = "00000000-0000-0000-0000-000000000000"

PRINT_RESULT = "snowflake.cli._plugins.sql.snowsql_commands.print_result"


def test_result_from_args():
    assert ResultCommand.from_args(["incorrect_id"], {}) == CompileCommandResult(
        error_message="Invalid query ID passed to 'result' command: incorrect_id"
    )
    assert ResultCommand.from_args([], {}) == CompileCommandResult(
        error_message="No arguments passed to 'result' command. Usage: `!result <query id>`"
    )
    assert ResultCommand.from_args([1, 2], {}) == CompileCommandResult(
        error_message="Too many arguments passed to 'result' command. Usage: `!result <query id>`"
    )
    assert ResultCommand.from_args([], {"unwanted": "kwarg"}) == CompileCommandResult(
        error_message="Invalid argument passed to 'result' command: unwanted=kwarg"
    )
    assert ResultCommand.from_args([_FAKE_QID], {}) == CompileCommandResult(
        command=ResultCommand(_FAKE_QID)
    )


def test_abort_from_args():
    assert AbortCommand.from_args(["incorrect_id"], {}) == CompileCommandResult(
        error_message="Invalid query ID passed to 'abort' command: incorrect_id"
    )
    assert AbortCommand.from_args([], {}) == CompileCommandResult(
        error_message="No arguments passed to 'abort' command. Usage: `!abort <query id>`"
    )
    assert AbortCommand.from_args([1, 2], {}) == CompileCommandResult(
        error_message="Too many arguments passed to 'abort' command. Usage: `!abort <query id>`"
    )
    assert AbortCommand.from_args([], {"unwanted": "kwarg"}) == CompileCommandResult(
        error_message="Invalid argument passed to 'abort' command: unwanted=kwarg"
    )
    assert AbortCommand.from_args([_FAKE_QID], {}) == CompileCommandResult(
        command=AbortCommand(_FAKE_QID)
    )


@mock.patch(PRINT_RESULT)
def test_result_execute(mock_print, mock_ctx):
    command = ResultCommand(_FAKE_QID)
    ctx = mock_ctx()
    command.execute(ctx)
    ctx.cursor().query_result.assert_called_once_with(_FAKE_QID)
    mock_print.assert_called_once()


@mock.patch(PRINT_RESULT)
def test_abort_execute(mock_print, mock_ctx):
    command = AbortCommand(_FAKE_QID)
    ctx = mock_ctx()
    command.execute(ctx)
    ctx.cursor().execute.assert_called_once_with(
        f"SELECT SYSTEM$CANCEL_QUERY('{_FAKE_QID}')"
    )
    mock_print.assert_called_once()


def test_queries_from_args():
    # default values
    assert QueriesCommand.from_args([], {}) == (
        CompileCommandResult(
            command=QueriesCommand(amount=25, from_current_session=True)
        )
    )

    # session is set only if kwargs are empty
    assert QueriesCommand.from_args([], {"amount": "3"}) == (
        CompileCommandResult(
            command=QueriesCommand(amount=3, from_current_session=False)
        )
    )

    # unless "session" arg is provided
    assert QueriesCommand.from_args(["session"], {"amount": "3"}) == (
        CompileCommandResult(
            command=QueriesCommand(amount=3, from_current_session=True)
        )
    )

    # help
    assert QueriesCommand.from_args(["help", "session"], {"amount": 3}) == (
        CompileCommandResult(command=QueriesCommand(help_mode=True))
    )

    # all arguments
    assert QueriesCommand.from_args(
        ["session"],
        {
            "warehouse": "warehouse",
            "user": "user",
            "amount": "3",
            "start": "1234",
            "end": "5678",
            "duration": "200",
            "type": "insert",
            "status": "running",
        },
    ) == (
        CompileCommandResult(
            command=QueriesCommand(
                help_mode=False,
                from_current_session=True,
                amount=3,
                user="user",
                warehouse="warehouse",
                start_timestamp_ms=1234,
                end_timestamp_ms=5678,
                duration="200",
                stmt_type="INSERT",
                status="RUNNING",
            )
        )
    )

    # start_date and end_date conversion
    assert QueriesCommand.from_args(
        [],
        {
            "start_date": "2025-05-05T00:00:00+00:00",
            "end_date": "2025-05-05T00:00:01+00:00",
        },
    ) == CompileCommandResult(
        command=QueriesCommand(
            start_timestamp_ms=1746403200000, end_timestamp_ms=1746403201000
        )
    )

    # errors
    assert QueriesCommand.from_args(["unknown_arg"], {}) == (
        CompileCommandResult(
            error_message="Invalid argument passed to 'queries' command: unknown_arg"
        )
    )
    assert QueriesCommand.from_args([], {"unknown": "kwarg"}) == (
        CompileCommandResult(
            error_message="Invalid argument passed to 'queries' command: unknown=kwarg"
        )
    )
    assert QueriesCommand.from_args([], {"amount": "text"}) == (
        CompileCommandResult(
            error_message="Invalid argument passed to 'amount' filter: text"
        )
    )
    assert QueriesCommand.from_args([], {"type": "invalid"}) == (
        CompileCommandResult(
            error_message="Invalid argument passed to 'type' filter: INVALID"
        )
    )
    assert QueriesCommand.from_args([], {"status": "invalid"}) == (
        CompileCommandResult(
            error_message="Invalid argument passed to 'status' filter: INVALID"
        )
    )
    assert QueriesCommand.from_args([], {"start": "23456aba"}) == (
        CompileCommandResult(
            error_message="Invalid argument passed to 'start' filter: 23456aba"
        )
    )
    assert QueriesCommand.from_args([], {"end": "23456aba"}) == (
        CompileCommandResult(
            error_message="Invalid argument passed to 'end' filter: 23456aba"
        )
    )
    assert QueriesCommand.from_args([], {"start_date": "not-a-date"}) == (
        CompileCommandResult(
            error_message="Invalid date format passed to 'start_date' filter: not-a-date"
        )
    )
    assert QueriesCommand.from_args([], {"end_date": "not-a-date"}) == (
        CompileCommandResult(
            error_message="Invalid date format passed to 'end_date' filter: not-a-date"
        )
    )
    assert QueriesCommand.from_args([], {"start": 123, "start_date": "2025-05-01"}) == (
        CompileCommandResult(
            error_message="'start_date' filter cannot be used with 'start' filter"
        )
    )
    assert QueriesCommand.from_args([], {"end": 123, "end_date": "2025-05-01"}) == (
        CompileCommandResult(
            error_message="'end_date' filter cannot be used with 'end' filter"
        )
    )


@pytest.mark.parametrize(
    "status",
    ["RUNNING", "SUCCEEDED", "FAILED", "BLOCKED", "QUEUED", "ABORTED"],
)
def test_queries_status_values(status):
    assert QueriesCommand.from_args(
        [], {"status": status.capitalize()}
    ) == CompileCommandResult(command=QueriesCommand(status=status.upper()))


@pytest.mark.parametrize(
    "type_",
    [
        "ANY",
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "MULTI_TABLE_INSERT",
        "COPY",
        "COMMIT",
        "ROLLBACK",
        "BEGIN_TRANSACTION",
        "SHOW",
        "GRANT",
        "CREATE",
        "ALTER",
    ],
)
def test_queries_type_values(type_):
    assert QueriesCommand.from_args(
        [], {"type": type_.capitalize()}
    ) == CompileCommandResult(command=QueriesCommand(stmt_type=type_.upper()))


@mock.patch("time.time")
@mock.patch(PRINT_RESULT)
@pytest.mark.parametrize("current_session", [True, False])
def test_queries_execute(mock_print, mock_time, mock_ctx, current_session):
    mock_time.return_value = "mocked_time"
    ctx = mock_ctx()
    ctx.session_id = "mocked_session_id"
    QueriesCommand(
        help_mode=False,
        from_current_session=current_session,
        amount=3,
        user="user",
        warehouse="warehouse",
        start_timestamp_ms=2345,
        end_timestamp_ms=6789,
        duration="200",
        stmt_type="INSERT",
        status="RUNNING",
    ).execute(ctx)

    expected_url = (
        "/monitoring/queries?_dc=mocked_time&includeDDL=false&max=3&user=user&wh=warehouse"
        "&start=2345&end=6789&min_duration=200"
        f"{'&session_id=mocked_session_id' if current_session else ''}"
        "&subset=RUNNING&stmt_type=INSERT"
    )
    ctx.rest.request.assert_called_once_with(
        url=expected_url, method="get", client="rest"
    )
    mock_print.assert_called_once()


@mock.patch(PRINT_RESULT)
def test_queries_execute_help(mock_print, mock_ctx):
    ctx = mock_ctx()
    ctx.session_id = "mocked_session_id"
    QueriesCommand(help_mode=True).execute(ctx)

    ctx.rest.request.assert_not_called()
    mock_print.assert_called_once()


@pytest.mark.parametrize(
    "command,args,expected",
    [
        ("!result", [_FAKE_QID], ResultCommand(_FAKE_QID)),
        ("!abort", [_FAKE_QID], AbortCommand(_FAKE_QID)),
        ("!queries", ["amount=3", "user=jdoe"], QueriesCommand(amount=3, user="jdoe")),
        ("!QuERies", ["session"], QueriesCommand(from_current_session=True)),
        (
            "!ResUlT",
            [],
            "No arguments passed to 'result' command. Usage: `!result <query id>`",
        ),
        (
            "!AbORT",
            ["incorrect_id"],
            "Invalid query ID passed to 'abort' command: incorrect_id",
        ),
        ("!unknown", [], "Unknown command '!unknown'"),
    ],
)
def test_compile_commands(command, args, expected):
    if isinstance(expected, SnowSQLCommand):
        expected_result = CompileCommandResult(command=expected)
    else:
        expected_result = CompileCommandResult(error_message=expected)

    assert compile_snowsql_command(command=command, cmd_args=args) == expected_result
