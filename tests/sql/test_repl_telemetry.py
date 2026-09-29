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

from unittest import mock

import pytest
from snowflake.cli._plugins.sql.client_query_span import SQL_CLIENT_QUERY_SPAN
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.metrics import CLIMetrics, CLIMetricsSpan
from snowflake.cli.api.output.formats import OutputFormat

from tests.sql.conftest import run_repl, sql_output_settings


def _sql_spans():
    return [
        span
        for span in get_cli_context_manager().metrics.completed_spans
        if span[CLIMetricsSpan.NAME_KEY] == SQL_CLIENT_QUERY_SPAN
    ]


def test_repl_records_one_span_for_one_sync_query(repl):
    run_repl(repl, ("select 1;", "exit", "y"))

    spans = _sql_spans()
    assert len(spans) == 1
    assert spans[0][CLIMetricsSpan.ERROR_KEY] is None


def test_repl_records_one_span_for_multi_statement_input(repl, mock_cursor):
    cursors = [
        mock_cursor(rows=[("1",)], columns=["1"]),
        mock_cursor(rows=[("2",)], columns=["2"]),
    ]

    with mock.patch.object(repl, "_execute", return_value=(len(cursors), cursors)):
        run_repl(repl, ("select 1; select 2;", "exit", "y"))

    spans = _sql_spans()
    assert len(spans) == 1
    assert spans[0][CLIMetricsSpan.ERROR_KEY] is None


def test_repl_records_independent_spans_for_successive_queries(repl):
    run_repl(
        repl,
        ("select 1;", "select 2;", "exit", "y"),
        monotonic_values=(0.0, 0.1, 1.0, 1.4),
    )

    spans = _sql_spans()
    assert len(spans) == 2
    assert spans[0][CLIMetricsSpan.ERROR_KEY] is None
    assert spans[1][CLIMetricsSpan.ERROR_KEY] is None


def test_repl_skips_span_for_compile_error(repl):
    run_repl(repl, ("select <% missing %>;", "exit", "y"))

    assert _sql_spans() == []


def test_repl_skips_span_for_comment_only(repl):
    run_repl(repl, ("-- just a comment", "exit", "y"))

    assert _sql_spans() == []


def test_repl_skips_span_for_empty_input(repl):
    run_repl(repl, ("", "   ", "exit", "y"))

    assert _sql_spans() == []


def test_repl_skips_span_for_command_only(compiling_repl):
    run_repl(compiling_repl, ("!queries help", "exit", "y"))

    assert _sql_spans() == []


def test_repl_skips_span_for_async_only(compiling_repl):
    run_repl(compiling_repl, ("select 1;>", "exit", "y"))

    assert _sql_spans() == []


def test_repl_records_one_span_for_mixed_sync_and_async(compiling_repl):
    run_repl(compiling_repl, ("select 1; select 2;>", "exit", "y"))

    spans = _sql_spans()
    assert len(spans) == 1
    assert spans[0][CLIMetricsSpan.ERROR_KEY] is None


def test_repl_records_span_after_render_error(repl):
    with mock.patch(
        "snowflake.cli._plugins.sql.repl.print_result",
        side_effect=Exception("query failed"),
    ):
        run_repl(repl, ("select 1;", "exit", "y"))

    spans = _sql_spans()
    assert len(spans) == 1
    assert spans[0][CLIMetricsSpan.ERROR_KEY] == "Exception"


@pytest.mark.parametrize(
    "output_format,silent",
    (
        (OutputFormat.JSON, False),
        (OutputFormat.JSON_EXT, False),
        (OutputFormat.CSV, False),
        (OutputFormat.TABLE, True),
    ),
)
def test_repl_records_span_when_footer_is_suppressed(repl, output_format, silent):
    with sql_output_settings(output_format, silent=silent):
        run_repl(repl, ("select 1;", "exit", "y"))

    spans = _sql_spans()
    assert len(spans) == 1
    assert spans[0][CLIMetricsSpan.ERROR_KEY] is None


def test_repl_records_keyboard_interrupt_during_print_result(repl, mock_cursor):
    cursors = [mock_cursor(rows=[("2",)], columns=["2"])]

    with mock.patch(
        "snowflake.cli._plugins.sql.repl.print_result",
        side_effect=(KeyboardInterrupt, None),
    ), mock.patch.object(repl, "_execute", return_value=(len(cursors), cursors)):
        run_repl(
            repl,
            ("select 1;", "select 2;", "exit", "y"),
            monotonic_values=(0.0, 10.0, 10.25),
        )

    spans = _sql_spans()
    errors = [span[CLIMetricsSpan.ERROR_KEY] for span in spans]
    assert "KeyboardInterrupt" in errors
    assert None in errors


def test_repl_discards_span_when_execute_is_interrupted_before_cnt(repl):
    with mock.patch.object(repl, "_execute", side_effect=KeyboardInterrupt):
        run_repl(repl, ("select 1;", "exit", "y"))

    assert _sql_spans() == []


def test_repl_drops_spans_past_total_limit(repl, mock_cursor):
    n = CLIMetrics.SPAN_TOTAL_LIMIT + 1
    cursors = [mock_cursor(rows=[("1",)], columns=["1"])]

    with mock.patch.object(repl, "_execute", return_value=(1, cursors)):
        run_repl(
            repl,
            tuple(["select 1;"] * n) + ("exit", "y"),
            monotonic_values=tuple(float(i) for i in range(n * 2 + 8)),
        )

    assert len(_sql_spans()) == CLIMetrics.SPAN_TOTAL_LIMIT
    assert get_cli_context_manager().metrics.num_spans_past_total_limit == 1
