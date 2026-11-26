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

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from snowflake.cli._app.telemetry import CLITelemetryField, TelemetryEvent
from snowflake.cli.api.metrics import CLIMetrics, CLIMetricsSpan
from snowflake.connector.telemetry import TelemetryData

from tests_common.span_assert import CompletedSpans, span

NAME = CLIMetricsSpan.NAME_KEY
PARENT = CLIMetricsSpan.PARENT_KEY
ERROR = CLIMetricsSpan.ERROR_KEY
ERROR_CODE = CLIMetricsSpan.ERROR_CODE_KEY
ID = CLIMetricsSpan.ID_KEY
PARENT_ID = CLIMetricsSpan.PARENT_ID_KEY


def _span(
    name: str,
    parent: str | None = None,
    error=None,
    error_code=None,
    *,
    span_id: str | None = None,
    parent_id: str | None = None,
) -> dict:
    return {
        NAME: name,
        PARENT: parent,
        ERROR: error,
        ERROR_CODE: error_code,
        ID: span_id,
        PARENT_ID: parent_id,
    }


def _message(spans: list[dict], *, depth: int = 0, total: int = 0, event=None) -> dict:
    return {
        "type": (event or TelemetryEvent.CMD_EXECUTION_RESULT).value,
        CLITelemetryField.SPANS.value: {
            CLITelemetryField.COMPLETED_SPANS.value: spans,
            CLITelemetryField.NUM_SPANS_PAST_DEPTH_LIMIT.value: depth,
            CLITelemetryField.NUM_SPANS_PAST_TOTAL_LIMIT.value: total,
        },
    }


def _batch(*messages: dict) -> MagicMock:
    batch = MagicMock()
    for message in messages:
        data = MagicMock(spec=TelemetryData)
        data.to_dict.return_value = {"message": message}
        batch(data)
    return batch


CORE = _span("core.config.load", span_id="core")
LOAD = _span("core.plugins.load", span_id="load")
DEPLOY = _span("action.app.deploy", span_id="deploy")
PKG = _span(
    "action.app_pkg.deploy",
    "action.app.deploy",
    span_id="pkg",
    parent_id="deploy",
)
BUNDLE = _span(
    "bundle",
    "action.app_pkg.deploy",
    span_id="bundle",
    parent_id="pkg",
)
SIGHT = _span("get_snowsight_url_for_app", span_id="sight")
VALIDATE = _span("cmd.dbt.deploy.validate", span_id="validate")
UPLOAD = _span("cmd.dbt.deploy.upload", span_id="upload")
STAGE = _span(
    "cmd.dbt.deploy.upload.stage",
    "cmd.dbt.deploy.upload",
    span_id="stage",
    parent_id="upload",
)


def test_prefix_hides_spans_from_other_owners():
    spans = CompletedSpans([CORE, LOAD, VALIDATE, UPLOAD, STAGE]).prefix(
        "cmd.dbt.deploy."
    )

    spans.assert_exactly(
        span("cmd.dbt.deploy.validate"),
        span("cmd.dbt.deploy.upload"),
        span("cmd.dbt.deploy.upload.stage", parent="cmd.dbt.deploy.upload"),
    )
    spans.assert_absent("core.config.load")


def test_assert_exactly_fails_on_extra_span_inside_the_prefix():
    spans = CompletedSpans([VALIDATE, UPLOAD, STAGE]).prefix("cmd.dbt.deploy.")

    with pytest.raises(AssertionError, match="Span tree mismatch"):
        spans.assert_exactly(
            span("cmd.dbt.deploy.validate"),
            span("cmd.dbt.deploy.upload"),
        )


def test_assert_exactly_ignores_spans_outside_the_prefix():
    CompletedSpans([CORE, VALIDATE]).prefix("cmd.dbt.deploy.").assert_exactly(
        span("cmd.dbt.deploy.validate")
    )


def test_under_includes_descendants_and_excludes_sibling_roots():
    spans = CompletedSpans([CORE, DEPLOY, PKG, BUNDLE, SIGHT]).under(
        "action.app.deploy"
    )

    spans.assert_exactly(
        span("action.app.deploy"),
        span("action.app_pkg.deploy", parent="action.app.deploy"),
        span("bundle", parent="action.app_pkg.deploy"),
    )
    spans.assert_absent("get_snowsight_url_for_app", "core.config.load")


def test_under_keeps_multiple_roots():
    spans = CompletedSpans([DEPLOY, PKG, SIGHT]).under(
        "action.app.deploy", "get_snowsight_url_for_app"
    )

    spans.assert_present(
        "action.app.deploy", "get_snowsight_url_for_app", "action.app_pkg.deploy"
    )


def test_under_does_not_keep_same_named_span_from_another_tree():
    other_root = _span("action.app.validate", span_id="other-root")
    other_bundle = _span(
        "bundle",
        "action.app.validate",
        span_id="other-bundle",
        parent_id="other-root",
    )
    other_child = _span(
        "templates_processor",
        "bundle",
        span_id="other-child",
        parent_id="other-bundle",
    )
    hooks_pkg = _span(
        "post_deploy_hooks",
        "action.app_pkg.deploy",
        span_id="hooks-pkg",
        parent_id="pkg",
    )
    hooks_app = _span(
        "post_deploy_hooks",
        "update_app_object",
        span_id="hooks-app",
        parent_id="update",
    )
    update = _span(
        "update_app_object",
        "action.app.deploy",
        span_id="update",
        parent_id="deploy",
    )

    spans = CompletedSpans(
        [
            DEPLOY,
            PKG,
            update,
            BUNDLE,
            hooks_pkg,
            hooks_app,
            other_root,
            other_bundle,
            other_child,
        ]
    ).under("action.app.deploy")

    spans.assert_exactly(
        span("action.app.deploy"),
        span("action.app_pkg.deploy", parent="action.app.deploy"),
        span("update_app_object", parent="action.app.deploy"),
        span("bundle", parent="action.app_pkg.deploy"),
        span("post_deploy_hooks", parent="action.app_pkg.deploy"),
        span("post_deploy_hooks", parent="update_app_object"),
    )
    spans.assert_absent("action.app.validate", "templates_processor")


def test_under_follows_parent_id_not_parent_name():
    metrics = CLIMetrics()
    with metrics.span("action.app.deploy"):
        with metrics.span("bundle"):
            with metrics.span("in_tree_child"):
                pass
    with metrics.span("action.app.validate"):
        with metrics.span("bundle"):
            with metrics.span("out_of_tree_child"):
                pass

    CompletedSpans.from_metrics(metrics).under("action.app.deploy").assert_exactly(
        span("action.app.deploy"),
        span("bundle", parent="action.app.deploy"),
        span("in_tree_child", parent="bundle"),
    )


def test_named_filters_to_one_name():
    spans = CompletedSpans(
        [
            _span("core.plugins.load_failed", "core.plugins.load", "RuntimeError"),
            _span("core.plugins.load_failed", "core.plugins.load", "RuntimeError"),
            LOAD,
        ]
    ).named("core.plugins.load_failed")

    spans.assert_len(exactly=2)
    spans.assert_absent("core.plugins.load")
    spans.assert_each(parent="core.plugins.load", error="RuntimeError")


def test_get_lookup_ignores_unrelated_spans():
    span = CompletedSpans([CORE, VALIDATE]).get("cmd.dbt.deploy.validate")
    assert span[NAME] == "cmd.dbt.deploy.validate"


def test_get_requires_parent_when_the_name_is_not_unique():
    spans = CompletedSpans(
        [
            _span("post_deploy_hooks", "action.app_pkg.deploy"),
            _span("post_deploy_hooks", "update_app_object"),
        ]
    )

    with pytest.raises(AssertionError, match="is not unique"):
        spans.get("post_deploy_hooks")

    assert (
        spans.get("post_deploy_hooks", parent="update_app_object")[PARENT]
        == "update_app_object"
    )


def test_assert_present_and_absent():
    spans = CompletedSpans([VALIDATE, UPLOAD])
    spans.assert_present("cmd.dbt.deploy.validate", "cmd.dbt.deploy.upload")
    spans.assert_absent("cmd.dbt.deploy.cleanup")

    with pytest.raises(AssertionError, match="Missing"):
        spans.assert_present("cmd.dbt.deploy.cleanup")
    with pytest.raises(AssertionError, match="Unexpected"):
        spans.assert_absent("cmd.dbt.deploy.validate")


def test_assert_parent_error_and_clean():
    spans = CompletedSpans(
        [
            VALIDATE,
            STAGE,
            _span(
                "cmd.dbt.deploy.provision", error="ProgrammingError", error_code=3001
            ),
        ]
    )
    spans.assert_parent("cmd.dbt.deploy.upload.stage", "cmd.dbt.deploy.upload")
    spans.assert_error("cmd.dbt.deploy.provision", "ProgrammingError", code=3001)
    spans.assert_clean("cmd.dbt.deploy.validate")

    with pytest.raises(AssertionError, match="Expected no errors"):
        spans.assert_clean()


def test_from_message_checks_limit_counters():
    ok = _message([VALIDATE])
    CompletedSpans.from_message(ok).assert_present("cmd.dbt.deploy.validate")

    over = _message([VALIDATE], total=1)
    with pytest.raises(AssertionError):
        CompletedSpans.from_message(over)

    CompletedSpans.from_message(over, check_limits=False).assert_present(
        "cmd.dbt.deploy.validate"
    )


def test_from_result_reads_connection_and_mock_connect_shapes():
    batch = _batch(_message([VALIDATE]))
    connection = MagicMock()
    connection._telemetry.try_add_log_to_batch = batch  # noqa: SLF001
    CompletedSpans.from_result(connection).assert_present("cmd.dbt.deploy.validate")

    connect = MagicMock()
    connect.mocked_ctx._telemetry.try_add_log_to_batch = batch  # noqa: SLF001
    CompletedSpans.from_result(connect).assert_present("cmd.dbt.deploy.validate")


def test_from_result_uses_the_last_matching_event():
    first = _message([VALIDATE])
    second = _message([UPLOAD])
    spans = CompletedSpans.from_result(_batch(first, second))
    spans.assert_present("cmd.dbt.deploy.upload")
    spans.assert_absent("cmd.dbt.deploy.validate")


def test_from_error_reads_the_error_event():
    result = _message([VALIDATE])
    error = _message(
        [_span("cmd.dbt.deploy.validate", error="CliError")],
        event=TelemetryEvent.CMD_EXECUTION_ERROR,
    )
    CompletedSpans.from_error(_batch(result, error)).assert_error(
        "cmd.dbt.deploy.validate", "CliError"
    )


def test_from_metrics_reads_completed_spans():
    metrics = CLIMetrics()
    with metrics.span("outer_span"):
        pass
    CompletedSpans.from_metrics(metrics).assert_exactly(span("outer_span"))


def test_assert_len_reports_the_scoped_tree():
    spans = CompletedSpans([VALIDATE, UPLOAD]).prefix("cmd.dbt.deploy.")
    spans.assert_len(exactly=2)
    spans.assert_len(at_least=1)
    with pytest.raises(AssertionError, match="Expected 1 span, got 2"):
        spans.assert_len(exactly=1)
    with pytest.raises(AssertionError, match="Expected at least 3 spans, got 2"):
        spans.assert_len(at_least=3)
    with pytest.raises(ValueError, match="exactly"):
        spans.assert_len()
    with pytest.raises(ValueError, match="exactly"):
        spans.assert_len(exactly=2, at_least=1)


def test_assert_each_checks_every_span_in_the_view():
    spans = CompletedSpans(
        [
            _span("core.plugins.load_failed", "core.plugins.load", "RuntimeError"),
            _span("core.plugins.load_failed", "core.plugins.load", "RuntimeError"),
        ]
    )
    spans.assert_each(parent="core.plugins.load", error="RuntimeError")
    CompletedSpans([]).assert_each(error="RuntimeError")

    with pytest.raises(AssertionError, match="did not match"):
        spans.assert_each(parent="core.plugins.register")
    with pytest.raises(ValueError, match="parent"):
        spans.assert_each()


def test_prefix_and_under_require_arguments():
    spans = CompletedSpans([VALIDATE])
    with pytest.raises(ValueError, match="prefix"):
        spans.prefix()
    with pytest.raises(ValueError, match="under"):
        spans.under()
