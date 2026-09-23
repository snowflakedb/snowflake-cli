# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import sys
from unittest import mock

import pytest
from snowflake.cli._app.telemetry import CLITelemetryField, TelemetryEvent
from snowflake.cli._plugins.sql.client_query_span import SQL_CLIENT_QUERY_SPAN
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.metrics import CLIMetricsSpan
from snowflake.cli.api.sql_execution import VerboseCursor
from snowflake.connector import ProgrammingError


def _sql_spans():
    return [
        span
        for span in get_cli_context_manager().metrics.completed_spans
        if span[CLIMetricsSpan.NAME_KEY] == SQL_CLIENT_QUERY_SPAN
    ]


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_one_shot_query_records_client_query_span(mock_execute, runner, mock_cursor):
    mock_execute.return_value = (
        mock_cursor(rows=[("payload",)], columns=["c"]) for _ in range(1)
    )
    result = runner.invoke(["sql", "-q", "select 1"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("select 1", cursor_class=VerboseCursor)
    assert len(_sql_spans()) == 1
    assert _sql_spans()[0][CLIMetricsSpan.ERROR_KEY] is None
    assert result.output.count("payload") == 1
    assert "Time Elapsed" not in result.output


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_one_shot_file_records_client_query_span(
    mock_execute, runner, mock_cursor, named_temporary_file
):
    mock_execute.return_value = (
        mock_cursor(rows=[("payload",)], columns=["c"]) for _ in range(1)
    )
    query = "select 1"

    with named_temporary_file() as tmp_file:
        tmp_file.write_text(query)
        result = runner.invoke(["sql", "-f", tmp_file])

    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(query, cursor_class=VerboseCursor)
    assert len(_sql_spans()) == 1
    assert _sql_spans()[0][CLIMetricsSpan.ERROR_KEY] is None
    assert result.output.count("payload") == 1
    assert "Time Elapsed" not in result.output


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_one_shot_stdin_records_client_query_span(mock_execute, runner, mock_cursor):
    mock_execute.return_value = (
        mock_cursor(rows=[("payload",)], columns=["c"]) for _ in range(1)
    )
    result = runner.invoke(["sql", "-i"], input="select 1")
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("select 1", cursor_class=VerboseCursor)
    assert len(_sql_spans()) == 1
    assert _sql_spans()[0][CLIMetricsSpan.ERROR_KEY] is None
    assert result.output.count("payload") == 1
    assert "Time Elapsed" not in result.output


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_one_shot_multiple_statements_records_one_span(
    mock_execute, runner, mock_cursor
):
    mock_execute.side_effect = [
        (mock_cursor(rows=[("payload",)], columns=["c"]) for _ in range(1)),
        (mock_cursor(rows=[("payload",)], columns=["c"]) for _ in range(1)),
    ]
    result = runner.invoke(["sql", "-q", "select 1; select 2;"])
    assert result.exit_code == 0, result.output
    assert len(_sql_spans()) == 1
    assert _sql_spans()[0][CLIMetricsSpan.ERROR_KEY] is None
    assert result.output.count("payload") == 2
    assert "Time Elapsed" not in result.output


def test_one_shot_async_only_discards_span(runner):
    mock_cursor = mock.MagicMock()
    mock_cursor.sfqid = "01b0-async"
    mock_conn = mock.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    with mock.patch(
        "snowflake.cli._plugins.sql.manager.SqlManager.connection",
        new_callable=mock.PropertyMock,
        return_value=mock_conn,
    ):
        result = runner.invoke(["sql", "-q", "select 1;>"])
    assert result.exit_code == 0, result.output
    assert _sql_spans() == []


def test_one_shot_compile_error_discards_span(runner):
    result = runner.invoke(["sql", "-q", "select <% missing %>;"])
    assert result.exit_code != 0
    assert _sql_spans() == []


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_one_shot_query_error_records_span_error(mock_execute, runner):
    mock_execute.side_effect = ProgrammingError("exec failed")
    result = runner.invoke(["sql", "-q", "select 1"])
    assert result.exit_code != 0
    spans = _sql_spans()
    assert len(spans) == 1
    assert spans[0][CLIMetricsSpan.ERROR_KEY] is not None


@mock.patch("snowflake.cli._plugins.sql.repl.PromptSession")
@mock.patch("snowflake.cli._plugins.sql.repl.Repl._execute")
@mock.patch("snowflake.connector.connect")
def test_repl_exit_is_command_success(
    mock_conn, mock_execute, mock_prompt_session, runner, mock_cursor
):
    mock_execute.return_value = (1, (mock_cursor(["row"], []) for _ in range(1)))
    mock_prompt = mock.MagicMock()
    mock_prompt.prompt.side_effect = iter(("exit", "y"))
    mock_prompt_session.return_value = mock_prompt

    result = runner.invoke(["sql"])
    assert result.exit_code == 0, result.output

    events = [
        call.args[0].to_dict()
        for call in mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list  # noqa: SLF001
    ]
    result_events = [
        e for e in events if e["message"].get("type") == "result_executing_command"
    ]
    error_events = [
        e for e in events if e["message"].get("type") == "error_executing_command"
    ]
    assert result_events, events
    assert result_events[-1]["message"]["command_result_status"] == "success"
    assert not any(e["message"].get("error_type") == "SystemExit" for e in error_events)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "CliRunner plus DummyOutput does not reliably fire Click's result_callback, "
        "so the stderr_warning spy stays at 0 calls"
    ),
)
@mock.patch("snowflake.cli._app.version_check.cli_console.stderr_warning")
@mock.patch("snowflake.cli._app.version_check._banner_shown", False)
@mock.patch(
    "snowflake.cli._app.version_check.get_new_version_msg",
    return_value="upgrade-banner-probe",
)
@mock.patch("snowflake.cli._plugins.sql.repl.PromptSession")
@mock.patch("snowflake.cli._plugins.sql.repl.Repl._execute")
def test_repl_exit_may_show_upgrade_banner(
    mock_execute,
    mock_prompt_session,
    _msg,
    mock_stderr_warning,
    runner,
    mock_cursor,
):
    mock_execute.return_value = (1, (mock_cursor(["row"], []) for _ in range(1)))
    mock_prompt = mock.MagicMock()
    mock_prompt.prompt.side_effect = iter(("exit", "y"))
    mock_prompt_session.return_value = mock_prompt

    result = runner.invoke(["sql"])
    assert result.exit_code == 0, result.output
    # Banner is printed on stderr via Rich Console, which Click does not capture.
    mock_stderr_warning.assert_called_once_with("upgrade-banner-probe")


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_one_shot_span_on_result_payload_does_not_contain_sql(
    mock_execute, mock_conn, runner, mock_cursor
):
    sql = "select 'secret_literal_should_not_leak'"
    mock_execute.return_value = (
        mock_cursor(rows=[("payload",)], columns=["c"]) for _ in range(1)
    )
    result = runner.invoke(["sql", "-q", sql])
    assert result.exit_code == 0, result.output

    events = [
        call.args[0].to_dict()
        for call in mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list  # noqa: SLF001
    ]
    result_messages = [
        e["message"]
        for e in events
        if e["message"].get("type") == TelemetryEvent.CMD_EXECUTION_RESULT.value
    ]
    assert result_messages, events
    message = result_messages[-1]
    assert message.get(CLITelemetryField.COMMAND_EXECUTION_TIME.value)
    spans = message[CLITelemetryField.SPANS.value][
        CLITelemetryField.COMPLETED_SPANS.value
    ]
    names = [span[CLIMetricsSpan.NAME_KEY] for span in spans]
    assert SQL_CLIENT_QUERY_SPAN in names
    dumped = json.dumps(message)
    assert "secret_literal_should_not_leak" not in dumped
    assert sql not in dumped


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_one_shot_span_on_error_payload_does_not_contain_sql(
    mock_execute, mock_conn, runner
):
    sql = "select 'secret_literal_should_not_leak'"
    mock_execute.side_effect = ProgrammingError("exec failed")
    result = runner.invoke(["sql", "-q", sql])
    assert result.exit_code != 0

    events = [
        call.args[0].to_dict()
        for call in mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list  # noqa: SLF001
    ]
    error_messages = [
        e["message"]
        for e in events
        if e["message"].get("type") == TelemetryEvent.CMD_EXECUTION_ERROR.value
    ]
    assert error_messages, events
    message = error_messages[-1]
    spans = message[CLITelemetryField.SPANS.value][
        CLITelemetryField.COMPLETED_SPANS.value
    ]
    names = [span[CLIMetricsSpan.NAME_KEY] for span in spans]
    assert SQL_CLIENT_QUERY_SPAN in names
    dumped = json.dumps(message)
    assert "secret_literal_should_not_leak" not in dumped
    assert sql not in dumped
