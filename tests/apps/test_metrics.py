# Copyright (c) 2026 Snowflake Inc.
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

from unittest.mock import Mock, patch

import pytest
from snowflake.cli._plugins.apps.manager import AppStageManager, SnowflakeAppManager
from snowflake.cli._plugins.apps.metrics import (
    QUERY_IDS_KEY,
    install_query_id_telemetry,
    record_query_id,
)
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.metrics import CLIMetrics, CLIMetricsSpan
from snowflake.connector.errors import ProgrammingError

# Parent class whose ``execute_query`` both managers reach via ``super()``.
BASE_EXECUTE_QUERY = "snowflake.cli.api.sql_execution.BaseSqlExecutor.execute_query"


@pytest.fixture
def metrics() -> CLIMetrics:
    """Install a fresh ``CLIMetrics`` on the CLI context and restore after."""
    ctx_manager = get_cli_context_manager()
    original = ctx_manager.metrics
    fresh = CLIMetrics()
    ctx_manager.metrics = fresh
    try:
        yield fresh
    finally:
        ctx_manager.metrics = original


def _span_dict(metrics: CLIMetrics, name: str) -> dict:
    for span in metrics.completed_spans:
        if span[CLIMetricsSpan.NAME_KEY] == name:
            return span
    raise AssertionError(
        f"Span {name!r} not found in {[s[CLIMetricsSpan.NAME_KEY] for s in metrics.completed_spans]}"
    )


# ── record_query_id ───────────────────────────────────────────────────


def test_record_query_id_attaches_to_active_span(metrics):
    with metrics.span("snowflake_app.deploy") as span:
        record_query_id("qid-1")
        record_query_id("qid-2")
        assert metrics.current_span is span

    span_dict = _span_dict(metrics, "snowflake_app.deploy")
    assert span_dict[QUERY_IDS_KEY] == ["qid-1", "qid-2"]


def test_record_query_id_targets_innermost_span(metrics):
    with metrics.span("parent"):
        record_query_id("parent-q")
        with metrics.span("child"):
            record_query_id("child-q")
        record_query_id("parent-q2")

    assert _span_dict(metrics, "parent")[QUERY_IDS_KEY] == ["parent-q", "parent-q2"]
    assert _span_dict(metrics, "child")[QUERY_IDS_KEY] == ["child-q"]


def test_record_query_id_without_active_span_is_noop(metrics):
    record_query_id("orphan")
    assert metrics.completed_spans == []


@pytest.mark.parametrize("value", [None, ""])
def test_record_query_id_ignores_falsy_values(metrics, value):
    with metrics.span("snowflake_app.deploy"):
        record_query_id(value)

    span_dict = _span_dict(metrics, "snowflake_app.deploy")
    assert QUERY_IDS_KEY not in span_dict


def test_span_schema_unchanged_when_no_query_ids(metrics):
    """Spans that never record a query ID keep their original payload."""
    with metrics.span("snowflake_app.bundle"):
        pass

    span_dict = _span_dict(metrics, "snowflake_app.bundle")
    assert QUERY_IDS_KEY not in span_dict


def test_install_query_id_telemetry_is_idempotent():
    install_query_id_telemetry()
    wrapped = CLIMetricsSpan.to_dict
    install_query_id_telemetry()
    assert CLIMetricsSpan.to_dict is wrapped


# ── manager execute_query capture ─────────────────────────────────────


def test_snowflake_app_manager_records_sfqid(metrics):
    cursor = Mock()
    cursor.sfqid = "manager-qid"
    with patch(BASE_EXECUTE_QUERY, return_value=cursor) as mock_exec:
        with metrics.span("snowflake_app.deploy_service.create"):
            result = SnowflakeAppManager().execute_query("CREATE SERVICE foo")

    assert result is cursor
    mock_exec.assert_called_once()
    span_dict = _span_dict(metrics, "snowflake_app.deploy_service.create")
    assert span_dict[QUERY_IDS_KEY] == ["manager-qid"]


def test_snowflake_app_manager_records_sfqid_on_programming_error(metrics):
    err = ProgrammingError(msg="already exists")
    err.sfqid = "manager-err-qid"
    with patch(BASE_EXECUTE_QUERY, side_effect=err):
        with metrics.span("snowflake_app.deploy_service.create"):
            with pytest.raises(ProgrammingError):
                SnowflakeAppManager().execute_query("CREATE SERVICE foo")

    span_dict = _span_dict(metrics, "snowflake_app.deploy_service.create")
    assert span_dict[QUERY_IDS_KEY] == ["manager-err-qid"]


def test_app_stage_manager_records_sfqid(metrics):
    cursor = Mock()
    cursor.sfqid = "stage-qid"
    with patch(BASE_EXECUTE_QUERY, return_value=cursor):
        with metrics.span("snowflake_app.upload.push_stage_files"):
            AppStageManager().execute_query("PUT file://x @stage")

    span_dict = _span_dict(metrics, "snowflake_app.upload.push_stage_files")
    assert span_dict[QUERY_IDS_KEY] == ["stage-qid"]
