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

import pytest
from snowflake.cli._plugins.sql.client_query_span import (
    SQL_CLIENT_QUERY_SPAN,
    sql_client_query_span,
)
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.metrics import CLIMetricsSpan


def _completed_sql_spans():
    return [
        span
        for span in get_cli_context_manager().metrics.completed_spans
        if span[CLIMetricsSpan.NAME_KEY] == SQL_CLIENT_QUERY_SPAN
    ]


def test_helper_records_span_when_gate_is_kept():
    with sql_client_query_span() as gate:
        gate.record = True

    spans = _completed_sql_spans()
    assert len(spans) == 1
    assert spans[0][CLIMetricsSpan.NAME_KEY] == "sql.client_query"
    assert spans[0][CLIMetricsSpan.ERROR_KEY] is None
    assert spans[0][CLIMetricsSpan.EXECUTION_TIME_KEY] >= 0


def test_helper_discards_span_when_gate_is_left_false():
    with sql_client_query_span() as gate:
        assert gate.record is False

    assert _completed_sql_spans() == []


def test_helper_keeps_error_status_when_recorded():
    with pytest.raises(RuntimeError, match="boom"):
        with sql_client_query_span() as gate:
            gate.record = True
            raise RuntimeError("boom")

    spans = _completed_sql_spans()
    assert len(spans) == 1
    assert spans[0][CLIMetricsSpan.ERROR_KEY] == "RuntimeError"


def test_helper_discards_on_error_when_not_recorded():
    with pytest.raises(RuntimeError, match="boom"):
        with sql_client_query_span():
            raise RuntimeError("boom")

    assert _completed_sql_spans() == []


def test_helper_keeps_keyboard_interrupt_when_recorded():
    with pytest.raises(KeyboardInterrupt):
        with sql_client_query_span() as gate:
            gate.record = True
            raise KeyboardInterrupt

    spans = _completed_sql_spans()
    assert len(spans) == 1
    assert spans[0][CLIMetricsSpan.ERROR_KEY] == "KeyboardInterrupt"


def test_helper_defers_recorded_span_until_concluded():
    metrics = get_cli_context_manager().metrics
    with sql_client_query_span(defer_if_recorded=True) as gate:
        gate.record = True

    assert _completed_sql_spans() == []
    metrics.conclude_deferred_spans()
    assert len(_completed_sql_spans()) == 1


def test_helper_does_not_defer_on_error_even_if_requested():
    with pytest.raises(RuntimeError, match="boom"):
        with sql_client_query_span(defer_if_recorded=True) as gate:
            gate.record = True
            raise RuntimeError("boom")

    spans = _completed_sql_spans()
    assert len(spans) == 1
    assert spans[0][CLIMetricsSpan.ERROR_KEY] == "RuntimeError"
