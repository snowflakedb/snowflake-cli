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

import json
import shutil
from unittest import mock

import pytest
from snowflake.cli import __about__
from snowflake.cli.__about__ import CLIInstallationSource
from snowflake.cli._app import main_typer
from snowflake.cli._app.telemetry import TelemetryEvent
from snowflake.cli._plugins.sql.manager import SqlExecutionMixin
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.metrics import CLIMetrics
from snowflake.cli.api.plugins.command import (
    CommandPath,
    CommandSpec,
    CommandType,
)
from snowflake.cli.api.sql_execution import VerboseCursor
from typer import Typer

from tests_common.span_assert import CompletedSpans, span

STARTUP_PREFIXES = ("core.config.", "core.startup.", "core.plugins.")
CANARY = "canary_secret_xyz123"


def _reset_metrics() -> None:
    get_cli_context_manager().metrics = CLIMetrics()


def _startup_spans(mocked_ctx) -> CompletedSpans:
    return CompletedSpans.from_result(mocked_ctx).prefix(*STARTUP_PREFIXES)


def _result_message(mocked_ctx) -> dict:
    batch = mocked_ctx._telemetry.try_add_log_to_batch  # noqa: SLF001
    messages = [call.args[0].to_dict()["message"] for call in batch.call_args_list]
    matches = [
        m
        for m in messages
        if m.get("type") == TelemetryEvent.CMD_EXECUTION_RESULT.value
    ]
    assert matches, messages
    return matches[-1]


def _invoke_connection_test(runner, *args: str):
    return runner.invoke(["connection", "test", *args])


def _stub_sql(mock_execute, mock_cursor) -> None:
    mock_execute.return_value = (
        mock_cursor(rows=[("payload",)], columns=["c"]) for _ in range(1)
    )


@pytest.fixture(autouse=True)
def _fresh_metrics():
    _reset_metrics()
    yield


@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_pypi_success_carries_startup_spans(mock_om, mock_connect, runner):
    runner.invoke_via_typer(["connection", "test"])

    spans = _startup_spans(mock_connect.mocked_ctx)
    spans.assert_exactly(
        span("core.config.load"),
        span("core.config.load"),
        span("core.plugins.load"),
        span("core.plugins.register"),
    )
    spans.assert_clean()
    spans.assert_absent(
        "core.startup.auto_upgrade",
        "core.plugins.load_failed",
        "core.plugins.register_failed",
    )


@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_success_without_config_file_flag_has_one_config_load(
    mock_om, mock_connect, runner
):
    result = runner.super_invoke(["connection", "test"])
    assert result.exit_code == 0, result.output

    spans = _startup_spans(mock_connect.mocked_ctx)
    spans.named("core.config.load").assert_len(exactly=1)
    spans.assert_present("core.plugins.load", "core.plugins.register")
    spans.assert_clean()


@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_managed_install_emits_auto_upgrade_span(
    mock_om, mock_connect, runner, monkeypatch
):
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(main_typer, "_run_managed_auto_upgrade", lambda argv: None)

    runner.invoke_via_typer(["connection", "test"])

    spans = _startup_spans(mock_connect.mocked_ctx)
    spans.assert_exactly(
        span("core.config.load"),
        span("core.startup.auto_upgrade"),
        span("core.config.load"),
        span("core.plugins.load"),
        span("core.plugins.register"),
    )
    spans.assert_clean()


@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_auto_upgrade_fail_open_records_error(
    mock_om, mock_connect, runner, monkeypatch
):
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )

    def _boom(argv):
        raise RuntimeError("upgrade boom")

    boom = mock.Mock(side_effect=_boom)
    monkeypatch.setattr(main_typer, "_run_managed_auto_upgrade", boom)
    runner.invoke_via_typer(["connection", "test"])
    boom.assert_called()

    _startup_spans(mock_connect.mocked_ctx).assert_error(
        "core.startup.auto_upgrade", "RuntimeError"
    )


@mock.patch.object(SqlExecutionMixin, "_execute_string")
@mock.patch("snowflake.cli._plugins.connection.plugin_spec.command_spec")
def test_load_failed_marker_on_broken_command_spec(
    command_spec_mock, mock_execute, mock_connect, runner, mock_cursor
):
    command_spec_mock.side_effect = RuntimeError("Test exception")
    _stub_sql(mock_execute, mock_cursor)

    result = runner.invoke(["sql", "-q", "select 1"])
    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with("select 1", cursor_class=VerboseCursor)

    spans = _startup_spans(mock_connect.mocked_ctx).named("core.plugins.load_failed")
    spans.assert_len(at_least=1)
    spans.assert_each(parent="core.plugins.load", error="RuntimeError")


@mock.patch("pluggy.PluginManager.load_setuptools_entrypoints")
@mock.patch(
    "snowflake.cli.api.plugins.plugin_config.PluginConfigProvider.get_enabled_plugin_names"
)
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_load_failed_marker_on_broken_external_entrypoint(
    mock_om,
    enabled_plugin_names_mock,
    load_setuptools_entrypoints_mock,
    mock_connect,
    runner,
):
    enabled_plugin_names_mock.return_value = [CANARY]
    load_setuptools_entrypoints_mock.side_effect = RuntimeError(f"boom {CANARY}")

    result = _invoke_connection_test(runner)
    assert result.exit_code == 0, result.output

    message = _result_message(mock_connect.mocked_ctx)
    _startup_spans(mock_connect.mocked_ctx).assert_error(
        "core.plugins.load_failed",
        "RuntimeError",
        parent="core.plugins.load",
    )
    assert CANARY.lower() not in json.dumps(message).lower()


@mock.patch.object(SqlExecutionMixin, "_execute_string")
@mock.patch("snowflake.cli._plugins.connection.plugin_spec.command_spec")
def test_register_failed_marker(
    command_spec_mock, mock_execute, mock_connect, runner, mock_cursor
):
    command_spec_mock.return_value = CommandSpec(
        parent_command_path=CommandPath(["xyz123"]),
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=Typer(name="list", callback=lambda: None),
    )
    _stub_sql(mock_execute, mock_cursor)

    result = runner.invoke(["sql", "-q", "select 1"])
    assert result.exit_code == 0, result.output

    _startup_spans(mock_connect.mocked_ctx).assert_error(
        "core.plugins.register_failed",
        "RuntimeError",
        parent="core.plugins.register",
    )


@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_canary_config_path_is_not_in_events(
    mock_om, mock_connect, runner, tmp_path, test_snowcli_config
):
    canary_dir = tmp_path / CANARY
    canary_dir.mkdir()
    canary_config = canary_dir / "config.toml"
    shutil.copy(test_snowcli_config, canary_config)

    result = runner.invoke_with_config_file(str(canary_config), ["connection", "test"])
    assert result.exit_code == 0, result.output

    message = _result_message(mock_connect.mocked_ctx)
    _startup_spans(mock_connect.mocked_ctx)
    assert CANARY.lower() not in json.dumps(message).lower()
