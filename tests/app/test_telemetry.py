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
import os
import re
import uuid
from unittest import mock

import pytest
import typer
from click import ClickException
from snowflake.cli.api.config_provider import (
    ALTERNATIVE_CONFIG_ENV_VAR,
    reset_config_provider,
)
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import CouldNotUseObjectError
from snowflake.cli.api.feature_flags import BooleanFlag, FeatureFlag, FeatureFlagMixin
from snowflake.connector import ProgrammingError
from snowflake.connector.version import VERSION as DRIVER_VERSION

from tests_common.feature_flag_utils import with_feature_flags


class _TestFlags(FeatureFlagMixin):
    FOO = BooleanFlag("FOO", False)


@pytest.fixture(autouse=True)
def _no_devin_marker_file(monkeypatch, tmp_path):
    # Environment patches cannot hide a real marker file on a Devin machine.
    monkeypatch.setattr(
        "snowflake.cli._app.telemetry._DEVIN_MARKER_PATH",
        str(tmp_path / "no-devin-marker"),
    )


@mock.patch(
    "snowflake.cli._app.telemetry.python_version",
)
@mock.patch("snowflake.cli._app.telemetry.platform.platform")
@mock.patch("uuid.uuid4")
@mock.patch("snowflake.cli._app.telemetry.get_time_millis")
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
@with_feature_flags({_TestFlags.FOO: False})
def test_executing_command_sends_telemetry_usage_data_legacy_config(
    _, mock_conn, mock_time, mock_uuid4, mock_platform, mock_version, runner
):
    """Test telemetry with legacy config provider."""
    # Ensure legacy config is used
    with mock.patch.dict(os.environ, {}, clear=False):
        if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
            del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]
        reset_config_provider()

        mock_time.return_value = "123"
        mock_platform.return_value = "FancyOS"
        mock_version.return_value = "2.3.4"
        mock_uuid4.return_value = uuid.UUID("8a2225b3800c4017a4a9eab941db58fa")
        result = runner.invoke(["connection", "test"], catch_exceptions=False)
        assert result.exit_code == 0, result.output
        # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
        telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
        usage_command_event = (
            telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()
        )

        del usage_command_event["message"][
            "command_ci_environment"
        ]  # to avoid side effect from CI
        del usage_command_event["message"][
            "command_ci_integration_version"
        ]  # to avoid side effect from CI
        del usage_command_event["message"][
            "command_ci_auth_type"
        ]  # to avoid side effect from CI
        del usage_command_event["message"][
            "command_auth_type"
        ]  # to avoid side effect from resolved connection authenticator
        del usage_command_event["message"][
            "command_agent_environment"
        ]  # to avoid side effect from agent environment
        del usage_command_event["message"][
            "command_agent_session_id"
        ]  # to avoid side effect from CORTEX_SESSION_ID
        del usage_command_event["message"][
            "command_cortex_client_surface"
        ]  # to avoid side effect from CORTEX_CODE_CLIENT_SURFACE
        del usage_command_event["message"][
            "command_cortex_terminal_launcher"
        ]  # to avoid side effect from CORTEX_TERMINAL_LAUNCHER_SOURCE
        del usage_command_event["message"][
            "command_agent_indicators"
        ]  # to avoid side effect from agent environment
        del usage_command_event["message"][
            "command_stdin_tty"
        ]  # to avoid side effect from the test runner's terminal
        del usage_command_event["message"][
            "command_stdout_tty"
        ]  # to avoid side effect from the test runner's terminal
        assert usage_command_event == {
            "message": {
                "driver_type": "PythonConnector",
                "driver_version": ".".join(str(s) for s in DRIVER_VERSION[:3]),
                "source": "snowcli",
                "version_cli": "0.0.0-test_patched",
                "version_os": "FancyOS",
                "version_python": "2.3.4",
                "installation_source": "pypi",
                "command": ["connection", "test"],
                "command_group": "connection",
                "command_execution_id": "8a2225b3800c4017a4a9eab941db58fa",
                "command_flags": {"diag_log_path": "DEFAULT", "format": "DEFAULT"},
                "command_output_type": "TABLE",
                "command_agent_indicators_version": 1,
                "type": "executing_command",
                "project_definition_version": "None",
                "config_feature_flags": {
                    "dummy_flag": "True",
                    "foo": "False",
                    "wrong_type_flag": "UNKNOWN",
                },
                "config_provider_type": "legacy",
                "mode": "cmd",
            },
            "timestamp": "123",
        }


@mock.patch(
    "snowflake.cli._app.telemetry.python_version",
)
@mock.patch("snowflake.cli._app.telemetry.platform.platform")
@mock.patch("uuid.uuid4")
@mock.patch("snowflake.cli._app.telemetry.get_time_millis")
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
@with_feature_flags({_TestFlags.FOO: False})
def test_executing_command_sends_telemetry_usage_data_ng_config(
    _, mock_conn, mock_time, mock_uuid4, mock_platform, mock_version, runner
):
    """Test telemetry with NG config provider."""
    # Enable NG config
    with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "true"}):
        reset_config_provider()

        mock_time.return_value = "123"
        mock_platform.return_value = "FancyOS"
        mock_version.return_value = "2.3.4"
        mock_uuid4.return_value = uuid.UUID("8a2225b3800c4017a4a9eab941db58fa")
        result = runner.invoke(["connection", "test"], catch_exceptions=False)
        assert result.exit_code == 0, result.output

        # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
        telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
        usage_command_event = (
            telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()
        )

        del usage_command_event["message"][
            "command_ci_environment"
        ]  # to avoid side effect from CI
        del usage_command_event["message"][
            "command_ci_integration_version"
        ]  # to avoid side effect from CI
        del usage_command_event["message"][
            "command_ci_auth_type"
        ]  # to avoid side effect from CI
        del usage_command_event["message"][
            "command_auth_type"
        ]  # to avoid side effect from resolved connection authenticator
        del usage_command_event["message"][
            "command_agent_environment"
        ]  # to avoid side effect from agent environment
        del usage_command_event["message"][
            "command_agent_session_id"
        ]  # to avoid side effect from CORTEX_SESSION_ID
        del usage_command_event["message"][
            "command_cortex_client_surface"
        ]  # to avoid side effect from CORTEX_CODE_CLIENT_SURFACE
        del usage_command_event["message"][
            "command_cortex_terminal_launcher"
        ]  # to avoid side effect from CORTEX_TERMINAL_LAUNCHER_SOURCE

        # Verify common fields
        message = usage_command_event["message"]
        assert message["driver_type"] == "PythonConnector"
        assert message["source"] == "snowcli"
        assert message["version_cli"] == "0.0.0-test_patched"
        assert message["version_os"] == "FancyOS"
        assert message["version_python"] == "2.3.4"
        assert message["command"] == ["connection", "test"]
        assert message["command_group"] == "connection"
        assert message["type"] == "executing_command"
        assert message["config_provider_type"] == "ng"

        # Verify NG-specific config fields are present
        assert "config_sources_used" in message
        assert "config_source_wins" in message
        assert "config_total_keys_resolved" in message
        assert "config_keys_with_overrides" in message

        # These fields should be present (values will vary based on test config)
        assert isinstance(message["config_sources_used"], list)
        assert isinstance(message["config_source_wins"], dict)
        assert isinstance(message["config_total_keys_resolved"], int)
        assert isinstance(message["config_keys_with_overrides"], int)


@pytest.mark.parametrize(
    "ci_type, env_var",
    [
        ("SF_GITHUB_ACTION", "SF_GITHUB_ACTION"),
        ("SF_GITLAB_COMPONENT", "SF_GITLAB_COMPONENT"),
        ("SF_ADO_EXTENSION", "SF_ADO_EXTENSION"),
        ("GITHUB_ACTIONS", "GITHUB_ACTIONS"),
        ("GITLAB_CI", "GITLAB_CI"),
        ("CIRCLECI", "CIRCLECI"),
        ("JENKINS", "JENKINS_URL"),
        ("JENKINS", "HUDSON_URL"),
        ("AZURE_DEVOPS", "TF_BUILD"),
        ("BITBUCKET_PIPELINES", "BITBUCKET_BUILD_NUMBER"),
        ("AWS_CODEBUILD", "CODEBUILD_BUILD_ID"),
        ("TEAMCITY", "TEAMCITY_VERSION"),
        ("BUILDKITE", "BUILDKITE"),
        ("CODEFRESH", "CF_BUILD_ID"),
        ("TRAVIS_CI", "TRAVIS"),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_executing_command_sends_ci_usage_data(_, mock_conn, runner, env_var, ci_type):
    with mock.patch.dict(os.environ, {env_var: "true"}, clear=True):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    usage_command_event = (
        telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()
    )

    assert usage_command_event["message"]["command_ci_environment"] == ci_type


@pytest.mark.parametrize(
    "ci_value",
    ["true", "True", "TRUE", "1"],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_generic_ci_env_variable_returns_unknown_ci(_, mock_conn, runner, ci_value):
    """Test that generic CI=true/1 returns UNKNOWN_CI when no specific CI is detected."""
    with mock.patch.dict(os.environ, {"CI": ci_value}, clear=True):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    usage_command_event = (
        telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()
    )

    assert usage_command_event["message"]["command_ci_environment"] == "UNKNOWN_CI"


def test_get_ci_environment_type_returns_local_for_interactive_terminal():
    """Test that LOCAL is returned when running in an interactive terminal."""
    from snowflake.cli._app.telemetry import _get_ci_environment_type

    with mock.patch.dict(os.environ, {}, clear=True):
        with mock.patch(
            "snowflake.cli._app.telemetry.is_tty_interactive", return_value=True
        ):
            assert _get_ci_environment_type() == "LOCAL"


def test_get_ci_environment_type_returns_unknown_for_non_interactive_non_ci():
    """Test that UNKNOWN is returned when not in CI and not in an interactive terminal."""
    from snowflake.cli._app.telemetry import _get_ci_environment_type

    with mock.patch.dict(os.environ, {}, clear=True):
        with mock.patch(
            "snowflake.cli._app.telemetry.is_tty_interactive", return_value=False
        ):
            assert _get_ci_environment_type() == "UNKNOWN"


def test_detect_agent_environment_returns_unknown_when_no_agent():
    """Test that UNKNOWN is returned when no agent environment is detected."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, {}, clear=True):
        assert _detect_agent_environment() == "UNKNOWN"


@pytest.mark.parametrize(
    "env_var, env_value, expected_agent",
    [
        ("CORTEX_SESSION_ID", "abc123", "CORTEX"),
        ("CORTEX_CODE_CLIENT_SURFACE", "coco_desktop", "CORTEX"),
        ("CORTEX_TERMINAL_LAUNCHER_SOURCE", "sandbox", "CORTEX"),
        ("COCO_AGENT", "1", "CORTEX"),
        ("CURSOR_AGENT", "1", "CURSOR"),
        ("GEMINI_CLI", "1", "GEMINI_CLI"),
        ("CLAUDECODE", "1", "CLAUDE_CODE"),
        ("CODEX_API_KEY", "key123", "CODEX"),
        ("OPENCODE", "1", "OPENCODE"),
        ("OPENCODE_CLIENT", "web", "OPENCODE"),
        ("COPILOT_CLI", "1", "COPILOT_CLI"),
        ("CURSOR_EXTENSION_HOST_ROLE", "agent-exec", "CURSOR"),
        ("CLAUDE_CODE", "1", "CLAUDE_CODE"),
        ("CODEX_SANDBOX", "seatbelt", "CODEX"),
        ("CODEX_CI", "1", "CODEX"),
        ("CODEX_THREAD_ID", "thread-1", "CODEX"),
        ("CODEX_SANDBOX_NETWORK_DISABLED", "1", "CODEX"),
        ("KIMI_PLUGIN_ROOT", "/plugins/kimi", "KIMI"),
        ("GROK_AGENT", "1", "GROK"),
        ("GROK_PLUGIN_ROOT", "/plugins/grok", "GROK"),
        ("GROK_PLUGIN_DATA", "/data/grok", "GROK"),
        ("CLINE_ACTIVE", "true", "CLINE"),
        ("ANTIGRAVITY_AGENT", "1", "ANTIGRAVITY"),
        ("ANTIGRAVITY_CLI_ALIAS", "agy", "ANTIGRAVITY"),
        ("AUGMENT_AGENT", "1", "AUGMENT_CLI"),
        ("GOOSE_TERMINAL", "1", "GOOSE"),
        ("JUNIE_SHIM_PATH", "/usr/bin/junie", "JUNIE"),
        ("OPENCLAW_SHELL", "1", "OPENCLAW"),
    ],
)
def test_detect_agent_environment_returns_correct_agent(
    env_var, env_value, expected_agent
):
    """Test that the correct agent is detected based on environment variables."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, {env_var: env_value}, clear=True):
        assert _detect_agent_environment() == expected_agent


def test_detect_agent_environment_ignores_empty_marker_values():
    """Test that presence-based markers need a non-empty value."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, {"CLINE_ACTIVE": "  "}, clear=True):
        assert _detect_agent_environment() == "UNKNOWN"


def test_detect_agent_environment_cursor_extension_role_must_be_agent_exec():
    """Test that other CURSOR_EXTENSION_HOST_ROLE values are not treated as agents."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, {"CURSOR_EXTENSION_HOST_ROLE": "ui"}, clear=True):
        assert _detect_agent_environment() == "UNKNOWN"


@pytest.mark.parametrize(
    "env, expected_agent",
    [
        ({"CLAUDECODE": "1", "CODEX_API_KEY": "key"}, "CLAUDE_CODE"),
        ({"CLAUDECODE": "1", "GEMINI_CLI": "1"}, "CLAUDE_CODE"),
        (
            {"CLAUDECODE": "1", "CURSOR_EXTENSION_HOST_ROLE": "agent-exec"},
            "CURSOR",
        ),
        ({"OPENCODE": "1", "CODEX_API_KEY": "key"}, "OPENCODE"),
        ({"OPENCODE": "1", "CODEX_SANDBOX": "seatbelt"}, "OPENCODE"),
        ({"CODEX_API_KEY": "key", "COPILOT_CLI": "1"}, "CODEX"),
        ({"CODEX_SANDBOX": "seatbelt", "COPILOT_CLI": "1"}, "CODEX"),
        ({"CLAUDE_CODE": "1", "GEMINI_CLI": "1"}, "CLAUDE_CODE"),
        ({"CLAUDE_CODE": "1", "CODEX_API_KEY": "key"}, "CLAUDE_CODE"),
        ({"KIMI_PLUGIN_ROOT": "/k", "GEMINI_CLI": "1"}, "GEMINI_CLI"),
        ({"GROK_AGENT": "1", "CLAUDECODE": "1"}, "GROK"),
        ({"GROK_PLUGIN_DATA": "/g", "CLAUDE_CODE": "1"}, "GROK"),
        (
            {"GROK_AGENT": "1", "CLAUDECODE": "1", "CLAUDE_CODE_IS_COWORK": "1"},
            "GROK",
        ),
        ({"CURSOR_AGENT": "1", "GROK_AGENT": "1"}, "CURSOR"),
        ({"COPILOT_MODEL": "gpt-5", "GEMINI_CLI": "1"}, "GEMINI_CLI"),
    ],
)
def test_detect_agent_environment_table_order_wins(env, expected_agent):
    """Test that the first matching agent in _AGENT_RULES is reported."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, env, clear=True):
        assert _detect_agent_environment() == expected_agent


def test_detect_agent_environment_cowork_wins_over_claude_code():
    """Test that Claude Cowork is reported instead of Claude Code."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(
        os.environ,
        {"CLAUDECODE": "1", "CLAUDE_CODE_IS_COWORK": "1"},
        clear=True,
    ):
        assert _detect_agent_environment() == "COWORK"


def test_detect_agent_environment_cowork_requires_claude_code_marker():
    """Test that CLAUDE_CODE_IS_COWORK alone does not identify Cowork."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, {"CLAUDE_CODE_IS_COWORK": "1"}, clear=True):
        assert _detect_agent_environment() == "UNKNOWN"


@pytest.mark.parametrize(
    "stdin_tty, stdout_tty, expected_agent",
    [
        (False, False, "KIRO"),
        (True, False, "KIRO"),
        (False, True, "UNKNOWN"),
        (True, True, "UNKNOWN"),
    ],
)
def test_detect_agent_environment_kiro_requires_stdout_not_a_tty(
    stdin_tty, stdout_tty, expected_agent
):
    """Test that Kiro depends on stdout only, so redirecting stdin does not count."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, {"TERM_PROGRAM": "kiro"}, clear=True):
        with mock.patch(
            "snowflake.cli._app.telemetry.is_stdin_tty", return_value=stdin_tty
        ), mock.patch(
            "snowflake.cli._app.telemetry.is_stdout_tty", return_value=stdout_tty
        ):
            assert _detect_agent_environment() == expected_agent


def test_detect_agent_environment_detects_pi_from_path():
    """Test that Pi is detected from a .pi/agent segment in PATH."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(
        os.environ, {"PATH": "/usr/bin:/home/user/.pi/agent/bin"}, clear=True
    ):
        assert _detect_agent_environment() == "PI"


def test_detect_agent_environment_detects_devin_from_marker_file():
    """Test that Devin is detected from its marker file."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, {}, clear=True):
        with mock.patch(
            "snowflake.cli._app.telemetry.os.path.exists", return_value=True
        ):
            assert _detect_agent_environment() == "DEVIN"


@pytest.mark.parametrize(
    "ai_agent_value",
    ["my-tool", "my-tool@2.0", "custom agent with spaces"],
)
def test_detect_agent_environment_reports_other_for_ai_agent_variable(
    ai_agent_value,
):
    """Test that a custom agent setting AI_AGENT is reported as OTHER.

    The raw value is free text, so it is never returned as the agent name.
    """
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, {"AI_AGENT": ai_agent_value}, clear=True):
        assert _detect_agent_environment() == "OTHER"


def test_detect_agent_environment_specific_agent_wins_over_ai_agent():
    """Test that a specific agent marker is reported instead of AI_AGENT."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(
        os.environ, {"AI_AGENT": "my-tool", "CLAUDECODE": "1"}, clear=True
    ):
        assert _detect_agent_environment() == "CLAUDE_CODE"


def test_detect_agent_environment_ignores_empty_ai_agent():
    """Test that an empty AI_AGENT does not count as an agent."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, {"AI_AGENT": ""}, clear=True):
        assert _detect_agent_environment() == "UNKNOWN"


def test_is_coding_agent_true_for_ai_agent_variable():
    """Test that AI_AGENT makes is_coding_agent true, and unset makes it false."""
    from snowflake.cli._app.telemetry import is_coding_agent

    with mock.patch.dict(os.environ, {"AI_AGENT": "my-tool"}, clear=True):
        assert is_coding_agent() is True
    with mock.patch.dict(os.environ, {}, clear=True):
        assert is_coding_agent() is False


@pytest.mark.parametrize(
    "env_var, env_value",
    [
        ("CURSOR_TRACE_ID", "1"),
        # Not checked at all: set for ordinary GitHub authentication.
        ("COPILOT_GITHUB_TOKEN", "1"),
        ("COPILOT_MODEL", "gpt-5"),
        ("COPILOT_ALLOW_ALL", "true"),
        ("GOOSE_PROVIDER", "openai"),
        ("JUNIE_DATA", "/home/user/junie"),
        ("REPL_ID", "repl-123"),
        ("CLAUDE_CODE_SESSION_ID", "sess"),
        # Only the value "1" marks an agent terminal.
        ("GROK_AGENT", "reviewer"),
        ("GOOSE_TERMINAL", "true"),
    ],
)
def test_detect_agent_environment_skips_markers_present_in_human_sessions(
    env_var, env_value
):
    """Test that markers present in ordinary human sessions are not used for detection."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, {env_var: env_value}, clear=True):
        assert _detect_agent_environment() == "UNKNOWN"


@pytest.mark.parametrize(
    "env, expected_indicator",
    [
        ({"CORTEX_SESSION_ID": ""}, "cortex.session_id"),
        ({"CORTEX_CODE_CLIENT_SURFACE": "coco_cli"}, "cortex.client_surface"),
        ({"CORTEX_TERMINAL_LAUNCHER_SOURCE": "sandbox"}, "cortex.terminal_launcher"),
        ({"COCO_AGENT": "true"}, "cortex.coco_agent"),
        ({"CURSOR_AGENT": "1"}, "cursor.agent"),
        ({"CURSOR_EXTENSION_HOST_ROLE": "agent-exec"}, "cursor.agent_exec"),
        ({"CURSOR_TRACE_ID": "trace"}, "cursor.ide_terminal"),
        ({"CLAUDE_CODE_IS_COWORK": "1"}, "claude_code.cowork"),
        ({"CLAUDECODE": "1"}, "claude_code.claudecode"),
        ({"CLAUDE_CODE": "1"}, "claude_code.claude_code"),
        ({"CLAUDE_CODE_SESSION_ID": "sess"}, "claude_code.session_id"),
        ({"GEMINI_CLI": "1"}, "gemini_cli.flag"),
        ({"OPENCODE": "1"}, "opencode.flag"),
        ({"OPENCODE_CLIENT": "web"}, "opencode.client"),
        ({"CODEX_API_KEY": ""}, "codex.api_key"),
        ({"CODEX_SANDBOX": "seatbelt"}, "codex.sandbox"),
        ({"CODEX_CI": "1"}, "codex.ci"),
        ({"CODEX_THREAD_ID": "t"}, "codex.thread"),
        ({"CODEX_SANDBOX_NETWORK_DISABLED": "1"}, "codex.sandbox_network_disabled"),
        ({"COPILOT_CLI": "1"}, "copilot_cli.flag"),
        ({"COPILOT_MODEL": "gpt-5"}, "copilot_cli.model_setting"),
        ({"COPILOT_ALLOW_ALL": "true"}, "copilot_cli.allow_all_setting"),
        ({"KIMI_PLUGIN_ROOT": "/k"}, "kimi.plugin_hook"),
        ({"GROK_AGENT": "1"}, "grok.agent_terminal"),
        ({"GROK_PLUGIN_ROOT": "/g"}, "grok.plugin_root"),
        ({"GROK_PLUGIN_DATA": "/g"}, "grok.plugin_data"),
        ({"CLINE_ACTIVE": "true"}, "cline.active"),
        ({"ANTIGRAVITY_AGENT": "1"}, "antigravity.agent"),
        ({"ANTIGRAVITY_CLI_ALIAS": "agy"}, "antigravity.cli_alias"),
        ({"AUGMENT_AGENT": "1"}, "augment.agent"),
        ({"GOOSE_TERMINAL": "1"}, "goose.terminal"),
        ({"GOOSE_PROVIDER": "openai"}, "goose.provider_setting"),
        ({"JUNIE_DATA": "/j"}, "junie.data_setting"),
        ({"JUNIE_SHIM_PATH": "/j"}, "junie.shim"),
        ({"PATH": r"C:\Windows;C:\Users\u\.pi\agent\bin"}, "pi.path"),
        ({"REPL_ID": "r"}, "replit.repl"),
        ({"TERM_PROGRAM": "Kiro"}, "kiro.term_program"),
        ({"OPENCLAW_SHELL": "1"}, "openclaw.shell"),
        ({"AI_AGENT": "my-tool"}, "ai_agent.declared"),
    ],
)
def test_observe_agent_indicators_reports_single_indicator(env, expected_indicator):
    """Test that each variable produces exactly its own indicator token."""
    from snowflake.cli._app.telemetry import _observe_agent_indicators

    with mock.patch.dict(os.environ, env, clear=True):
        assert _observe_agent_indicators() == {expected_indicator}


def test_observe_agent_indicators_reports_devin_marker_file():
    from snowflake.cli._app.telemetry import _observe_agent_indicators

    with mock.patch.dict(os.environ, {}, clear=True):
        with mock.patch(
            "snowflake.cli._app.telemetry.os.path.exists", return_value=True
        ):
            assert _observe_agent_indicators() == {"devin.marker_file"}


def test_agent_indicator_tokens_are_unique_fixed_names():
    """Test that tokens are lowercase <agent>.<signal> names, never variable names."""
    from snowflake.cli._app.telemetry import _AGENT_INDICATORS

    tokens = [token for token, _ in _AGENT_INDICATORS]
    assert len(tokens) == len(set(tokens))
    for token in tokens:
        assert re.fullmatch(r"[a-z0-9_]+\.[a-z0-9_]+", token), token


@pytest.mark.parametrize(
    "env, expected",
    [
        ({"CORTEX_SESSION_ID": "abc123"}, "abc123"),
        ({"CLAUDECODE": "1", "CLAUDE_CODE_SESSION_ID": "claude-sess"}, "claude-sess"),
        (
            {
                "CLAUDECODE": "1",
                "CLAUDE_CODE_IS_COWORK": "1",
                "CLAUDE_CODE_SESSION_ID": "cowork-sess",
            },
            "cowork-sess",
        ),
        ({}, ""),
        ({"CLAUDECODE": "1"}, ""),
        ({"CLAUDE_CODE_SESSION_ID": "orphan"}, ""),
        ({"CORTEX_SESSION_ID": "  abc123  "}, "abc123"),
        (
            {"CLAUDECODE": "1", "CLAUDE_CODE_SESSION_ID": "  claude-sess  "},
            "claude-sess",
        ),
        (
            {
                "CORTEX_SESSION_ID": "cortex-id",
                "CLAUDECODE": "1",
                "CLAUDE_CODE_SESSION_ID": "claude-id",
            },
            "cortex-id",
        ),
    ],
)
def test_get_agent_session_id(env, expected):
    """Session id follows the detected agent; Cortex wins when both are set."""
    from snowflake.cli._app.telemetry import _get_agent_session_id

    with mock.patch.dict(os.environ, env, clear=True):
        assert _get_agent_session_id() == expected


@pytest.mark.parametrize(
    "env, expected_agent, expected_session_id",
    [
        ({"CORTEX_SESSION_ID": "abc123"}, "CORTEX", "abc123"),
        (
            {"CLAUDECODE": "1", "CLAUDE_CODE_SESSION_ID": "claude-sess"},
            "CLAUDE_CODE",
            "claude-sess",
        ),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_agent_session_id_appears_in_telemetry(
    _, mock_conn, runner, env, expected_agent, expected_session_id
):
    """The detected agent's session ID is included in the telemetry payload."""
    with mock.patch.dict(os.environ, env, clear=True):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    usage_command_event = (
        telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()
    )

    assert usage_command_event["message"]["command_agent_environment"] == expected_agent
    assert (
        usage_command_event["message"]["command_agent_session_id"]
        == expected_session_id
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_agent_session_id_empty_in_telemetry_when_unset(_, mock_conn, runner):
    """command_agent_session_id is present and empty when no agent session."""
    with mock.patch.dict(os.environ, {}, clear=True):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    usage_command_event = (
        telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()
    )

    assert usage_command_event["message"]["command_agent_session_id"] == ""


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_agent_indicators_and_tty_appear_in_telemetry(_, mock_conn, runner):
    """All observed indicators are sent, not only the ones that decide the agent."""
    from snowflake.cli._app.telemetry import AGENT_INDICATORS_VERSION

    env = {
        "GROK_AGENT": "1",
        "CLAUDECODE": "1",
        "CLAUDE_CODE_SESSION_ID": "parent-session",
        "COPILOT_MODEL": "gpt-5",
    }
    with mock.patch.dict(os.environ, env, clear=True), mock.patch(
        "snowflake.cli._app.telemetry.is_stdin_tty", return_value=True
    ), mock.patch("snowflake.cli._app.telemetry.is_stdout_tty", return_value=False):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    message = (
        telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()["message"]
    )
    assert message["command_agent_environment"] == "GROK"
    assert message["command_agent_session_id"] == ""
    assert message["command_agent_indicators"] == [
        "claude_code.claudecode",
        "claude_code.session_id",
        "copilot_cli.model_setting",
        "grok.agent_terminal",
    ]
    assert message["command_agent_indicators_version"] == AGENT_INDICATORS_VERSION
    assert message["command_stdin_tty"] is True
    assert message["command_stdout_tty"] is False


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_agent_indicators_empty_in_telemetry_when_unset(_, mock_conn, runner):
    with mock.patch.dict(os.environ, {}, clear=True):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    message = (
        telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()["message"]
    )
    assert message["command_agent_environment"] == "UNKNOWN"
    assert message["command_agent_indicators"] == []


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_agent_indicator_values_are_never_sent(_, mock_conn, runner):
    """No value of a checked variable reaches telemetry.

    Session ID variables are left out: command_agent_session_id sends them on purpose.
    """
    from snowflake.cli._app.telemetry import _AGENT_INDICATORS

    free_text_vars = [
        "CURSOR_TRACE_ID",
        "CLAUDE_CODE_IS_COWORK",
        "CLAUDE_CODE",
        "OPENCODE_CLIENT",
        "CODEX_API_KEY",
        "CODEX_SANDBOX",
        "CODEX_CI",
        "CODEX_THREAD_ID",
        "CODEX_SANDBOX_NETWORK_DISABLED",
        "COPILOT_MODEL",
        "COPILOT_ALLOW_ALL",
        "KIMI_PLUGIN_ROOT",
        "GROK_PLUGIN_ROOT",
        "GROK_PLUGIN_DATA",
        "CLINE_ACTIVE",
        "ANTIGRAVITY_AGENT",
        "ANTIGRAVITY_CLI_ALIAS",
        "AUGMENT_AGENT",
        "GOOSE_PROVIDER",
        "JUNIE_DATA",
        "JUNIE_SHIM_PATH",
        "REPL_ID",
        "OPENCLAW_SHELL",
        "AI_AGENT",
    ]
    env = {name: f"secret-value-{i}" for i, name in enumerate(free_text_vars)}
    env.update(
        {
            "COCO_AGENT": "1",
            "CURSOR_AGENT": "1",
            "CURSOR_EXTENSION_HOST_ROLE": "agent-exec",
            "CLAUDECODE": "1",
            "GEMINI_CLI": "1",
            "OPENCODE": "1",
            "COPILOT_CLI": "1",
            "GROK_AGENT": "1",
            "GOOSE_TERMINAL": "1",
            "PATH": "/home/secret-value-path/.pi/agent/bin",
            "TERM_PROGRAM": "kiro-secret-value-term",
        }
    )
    with mock.patch.dict(os.environ, env, clear=True):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    calls = telemetry.try_add_log_to_batch.call_args_list
    assert calls
    sent = json.dumps([call.args[0].to_dict() for call in calls], default=str)
    assert "secret-value" not in sent

    indicators = calls[0].args[0].to_dict()["message"]["command_agent_indicators"]
    assert indicators == sorted(set(indicators))
    not_triggered = {
        "cortex.session_id",
        "cortex.client_surface",
        "cortex.terminal_launcher",
        "claude_code.session_id",
        "devin.marker_file",
    }
    assert set(indicators) == {token for token, _ in _AGENT_INDICATORS} - not_triggered


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("coco_cli", "coco_cli"),
        ("coco_desktop", "coco_desktop"),
        ("other_host", "other_host"),
        ("  CoCo_Desktop  ", "coco_desktop"),
        ("sandbox", "sandbox"),
        ("", ""),
        ("not a real surface", ""),
        ("https://example.com", ""),
        ("-leading-hyphen", ""),
        ("x" * 64, "x" * 64),
        ("x" * 80, "x" * 64),
    ],
)
def test_sanitize_cortex_telemetry_token(raw, expected):
    from snowflake.cli._app.telemetry import _sanitize_cortex_telemetry_token

    assert _sanitize_cortex_telemetry_token(raw) == expected


@pytest.mark.parametrize(
    "env, expected_surface, expected_launcher",
    [
        ({}, "", ""),
        ({"CORTEX_CODE_CLIENT_SURFACE": "coco_cli"}, "coco_cli", ""),
        ({"CORTEX_CODE_CLIENT_SURFACE": "coco_desktop"}, "coco_desktop", ""),
        ({"CORTEX_CODE_CLIENT_SURFACE": "other_host"}, "other_host", ""),
        (
            {"CORTEX_TERMINAL_LAUNCHER_SOURCE": "sandbox"},
            "",
            "sandbox",
        ),
        (
            {
                "CORTEX_CODE_CLIENT_SURFACE": "coco_cli",
                "CORTEX_TERMINAL_LAUNCHER_SOURCE": "sandbox",
            },
            "coco_cli",
            "sandbox",
        ),
        ({"CORTEX_CODE_CLIENT_SURFACE": "not a real surface"}, "", ""),
        ({"CORTEX_CODE_CLIENT_SURFACE": "  CoCo_Desktop  "}, "coco_desktop", ""),
    ],
)
def test_cortex_surface_and_launcher_from_env(env, expected_surface, expected_launcher):
    from snowflake.cli._app.telemetry import (
        _get_cortex_client_surface,
        _get_cortex_terminal_launcher,
    )

    with mock.patch.dict(os.environ, env, clear=True):
        assert _get_cortex_client_surface() == expected_surface
        assert _get_cortex_terminal_launcher() == expected_launcher


@pytest.mark.parametrize(
    "env, expected_agent, expected_session_id, expected_surface, expected_launcher",
    [
        (
            {
                "CORTEX_SESSION_ID": "conv-1",
                "CORTEX_CODE_CLIENT_SURFACE": "coco_cli",
            },
            "CORTEX",
            "conv-1",
            "coco_cli",
            "",
        ),
        (
            {"CORTEX_CODE_CLIENT_SURFACE": "coco_desktop"},
            "CORTEX",
            "",
            "coco_desktop",
            "",
        ),
        (
            {"CORTEX_TERMINAL_LAUNCHER_SOURCE": "sandbox"},
            "CORTEX",
            "",
            "",
            "sandbox",
        ),
        (
            {
                "CORTEX_CODE_CLIENT_SURFACE": "coco_cli",
                "CORTEX_TERMINAL_LAUNCHER_SOURCE": "sandbox",
            },
            "CORTEX",
            "",
            "coco_cli",
            "sandbox",
        ),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_cortex_surface_appears_in_telemetry(
    _,
    mock_conn,
    runner,
    env,
    expected_agent,
    expected_session_id,
    expected_surface,
    expected_launcher,
):
    with mock.patch.dict(os.environ, env, clear=True):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    usage_command_event = (
        telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()
    )

    message = usage_command_event["message"]
    assert message["command_agent_environment"] == expected_agent
    assert message["command_agent_session_id"] == expected_session_id
    assert message["command_cortex_client_surface"] == expected_surface
    assert message["command_cortex_terminal_launcher"] == expected_launcher


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_cortex_surface_empty_in_telemetry_when_unset(_, mock_conn, runner):
    with mock.patch.dict(os.environ, {}, clear=True):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    usage_command_event = (
        telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()
    )

    assert usage_command_event["message"]["command_cortex_client_surface"] == ""
    assert usage_command_event["message"]["command_cortex_terminal_launcher"] == ""


def test_agent_context_non_tty_with_agent_detected():
    """Test the typical AI agent scenario: non-TTY terminal with agent env var set.

    In this case, CI detection should return UNKNOWN (not LOCAL, since no TTY),
    and agent detection should return the detected agent.
    """
    from snowflake.cli._app.telemetry import (
        _detect_agent_environment,
        _get_ci_environment_type,
    )

    with mock.patch.dict(os.environ, {"CURSOR_AGENT": "1"}, clear=True):
        with mock.patch(
            "snowflake.cli._app.telemetry.is_tty_interactive", return_value=False
        ):
            assert _get_ci_environment_type() == "UNKNOWN"
            assert _detect_agent_environment() == "CURSOR"


@mock.patch(
    "snowflake.cli._app.telemetry.python_version",
)
@mock.patch("snowflake.cli._app.telemetry.platform.platform")
@mock.patch("uuid.uuid4")
@mock.patch("snowflake.connector.time_util.get_time_millis")
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
@with_feature_flags({_TestFlags.FOO: False})
def test_executing_command_sends_telemetry_result_data(
    _, mock_conn, mock_time, mock_uuid4, mock_platform, mock_version, runner
):
    mock_time.return_value = "123"
    mock_platform.return_value = "FancyOS"
    mock_version.return_value = "2.3.4"
    mock_uuid4.return_value = uuid.UUID("8a2225b3800c4017a4a9eab941db58fa")
    result = runner.invoke(["connection", "test"], catch_exceptions=False)
    assert result.exit_code == 0, result.output

    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    result_command_event = (
        telemetry.try_add_log_to_batch.call_args_list[1].args[0].to_dict()
    )
    assert (
        result_command_event["message"]["type"] == "result_executing_command"
        and result_command_event["message"]["command_result_status"] == "success"
        and result_command_event["message"]["command_execution_time"]
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.streamlit.commands.StreamlitEntity")
def test_executing_command_sends_project_definition_in_telemetry_data(
    mock_entity, mock_conn, project_directory, runner
):
    with project_directory("streamlit_full_definition_v2"):
        result = runner.invoke(["streamlit", "deploy"])
    assert result.exit_code == 0, result.output

    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    actual_call = mock_conn.return_value._telemetry.try_add_log_to_batch.call_args.args[  # noqa: SLF001
        0
    ].to_dict()
    assert actual_call["message"]["project_definition_version"] == "2"


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_app_flow_is_absent_for_non_app_commands(_, mock_conn, runner):
    """Commands outside the ``snow app *`` group must never emit
    ``app_flow``. Check every telemetry call, not just the first/last,
    so a regression that mis-attaches the field anywhere in the lifecycle
    surfaces here."""
    result = runner.invoke(["connection", "test"], catch_exceptions=False)
    assert result.exit_code == 0, result.output

    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    for call in telemetry.try_add_log_to_batch.call_args_list:
        message = call.args[0].to_dict()["message"]
        assert "app_flow" not in message, message


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.streamlit.commands.StreamlitEntity")
def test_app_flow_is_absent_for_streamlit_commands(
    mock_entity, mock_conn, project_directory, runner
):
    """Loading a project definition for a non-``snow app`` command must not
    leak ``app_flow`` into telemetry."""
    with project_directory("streamlit_full_definition_v2"):
        result = runner.invoke(["streamlit", "deploy"])
    assert result.exit_code == 0, result.output

    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    for call in telemetry.try_add_log_to_batch.call_args_list:
        message = call.args[0].to_dict()["message"]
        assert "app_flow" not in message, message


@mock.patch("snowflake.connector.connect")
def test_app_flow_native_app_in_telemetry_data(mock_conn, project_directory, runner):
    """``snow app *`` invocations against a Native App project must report
    ``app_flow=native_app``. The ``napp_post_deploy_missing_file`` fixture
    is a v1 Native App project, exercising the in-memory v1->v2 conversion
    path in ``force_project_definition_v2``.

    The flow is detected during command execution, after
    ``log_command_usage`` (the ``executing_command`` event) runs. We
    therefore check the ``error_executing_command`` event, which is
    emitted from ``post_execute`` after the routing decorator has
    resolved the flow. (Native App and Snowflake App Runtime events can
    be joined on ``command_execution_id``.)
    """
    with project_directory("napp_post_deploy_missing_file"):
        runner.invoke(["app", "run"], catch_exceptions=False)

    events = _get_telemetry_events(mock_conn)
    _assert_executing_event_has_no_app_flow(events)
    _assert_result_or_error_event_has_app_flow(events, "native_app")


# ── Comprehensive app_flow telemetry attribution tests ─────────────────


def _get_telemetry_events(mock_conn):
    """Extract all telemetry event dicts from the mock connector."""
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    return [
        call.args[0].to_dict() for call in telemetry.try_add_log_to_batch.call_args_list
    ]


def _assert_executing_event_has_no_app_flow(events):
    """The executing_command event fires before routing decorators run,
    so app_flow must be absent (matching the contract in telemetry.py)."""
    executing = [e for e in events if e["message"]["type"] == "executing_command"]
    assert executing, "Expected at least one executing_command event"
    for event in executing:
        assert (
            "app_flow" not in event["message"]
        ), f"executing_command must not contain app_flow, got: {event['message']}"


def _assert_result_or_error_event_has_app_flow(events, expected_flow):
    """The result or error event (whichever fires) must contain the
    expected app_flow value."""
    post_events = [
        e
        for e in events
        if e["message"]["type"]
        in ("result_executing_command", "error_executing_command")
    ]
    assert post_events, "Expected a result or error telemetry event"
    for event in post_events:
        assert event["message"].get("app_flow") == expected_flow, (
            f"Expected app_flow={expected_flow!r} on {event['message']['type']}, "
            f"got: {event['message'].get('app_flow')!r}"
        )


class TestAppFlowTelemetryAttribution:
    """Verify that all snow app code paths stamp the correct app_flow on
    result/error telemetry events, and that the executing_command event
    never contains app_flow (since routing resolves after pre_execute)."""

    @mock.patch("snowflake.connector.connect")
    def test_native_app_bundle(self, mock_conn, project_directory, runner):
        """with_app_flow_routing detects native_app from v1 project."""
        with project_directory("napp_post_deploy_missing_file"):
            runner.invoke(["app", "bundle"], catch_exceptions=False)

        events = _get_telemetry_events(mock_conn)
        _assert_executing_event_has_no_app_flow(events)
        _assert_result_or_error_event_has_app_flow(events, "native_app")

    @mock.patch("snowflake.connector.connect")
    def test_snowflake_app_bundle(self, mock_conn, project_directory, runner):
        """with_app_flow_routing detects snowflake_app from entity type."""
        with project_directory("snowflake_app_v2"):
            runner.invoke(["app", "bundle"], catch_exceptions=False)

        events = _get_telemetry_events(mock_conn)
        _assert_executing_event_has_no_app_flow(events)
        _assert_result_or_error_event_has_app_flow(events, "snowflake_app")

    @mock.patch("snowflake.connector.connect")
    def test_snowflake_app_deploy(self, mock_conn, project_directory, runner):
        """with_app_flow_routing detects snowflake_app for deploy.
        The command will fail trying to execute SQL (the mock connector
        returns no cursor results), but the error event still carries
        app_flow because routing resolves before the SQL call."""
        with project_directory("snowflake_app_v2"):
            try:
                runner.invoke(["app", "deploy"], catch_exceptions=False)
            except ValueError:
                pass  # execute_string returns empty iterator with mocked connector

        events = _get_telemetry_events(mock_conn)
        _assert_executing_event_has_no_app_flow(events)
        _assert_result_or_error_event_has_app_flow(events, "snowflake_app")

    @mock.patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @mock.patch("snowflake.connector.connect")
    def test_snowflake_app_setup(self, mock_conn, mock_manager, runner):
        """setup is Snowflake-Apps-only; must stamp snowflake_app directly."""
        mock_mgr = mock_manager.return_value
        mock_mgr.fetch_app_service_defaults.return_value = {}
        mock_mgr.get_personal_database.return_value = None

        runner.invoke(
            ["app", "setup", "--app-name", "test_app", "--dry-run"],
            catch_exceptions=False,
        )

        events = _get_telemetry_events(mock_conn)
        _assert_executing_event_has_no_app_flow(events)
        _assert_result_or_error_event_has_app_flow(events, "snowflake_app")

    @mock.patch("snowflake.connector.connect")
    def test_native_app_deploy(self, mock_conn, project_directory, runner):
        """with_app_flow_routing detects native_app for deploy."""
        with project_directory("napp_post_deploy_missing_file"):
            runner.invoke(["app", "deploy"], catch_exceptions=False)

        events = _get_telemetry_events(mock_conn)
        _assert_executing_event_has_no_app_flow(events)
        _assert_result_or_error_event_has_app_flow(events, "native_app")


@mock.patch("snowflake.connector.connect")
@mock.patch("uuid.uuid4")
@mock.patch("snowflake.cli._plugins.streamlit.commands.StreamlitManager")
def test_failing_executing_command_sends_telemetry_data(
    _, mock_uuid4, mock_conn, project_directory, runner
):
    mock_uuid4.return_value = uuid.UUID("8a2225b3800c4017a4a9eab941db58fa")
    with project_directory("napp_post_deploy_missing_file"):
        runner.invoke(["app", "run"], catch_exceptions=False)

    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    result_command_event = (
        telemetry.try_add_log_to_batch.call_args_list[1].args[0].to_dict()
    )
    assert (
        result_command_event["message"]["type"] == "error_executing_command"
        and result_command_event["message"]["error_type"] == "SourceNotFoundError"
        and result_command_event["message"]["is_cli_exception"] == True
        and result_command_event["message"]["command_execution_id"]
        == "8a2225b3800c4017a4a9eab941db58fa"
    )


@pytest.mark.parametrize(
    "error,is_cli",
    [
        (ProgrammingError(), False),
        (ClickException("message"), True),
        (
            CouldNotUseObjectError(object_type=ObjectType.WAREHOUSE, name="warehouse"),
            True,
        ),
        (typer.Abort(), True),
        (typer.Exit(), True),
        (BrokenPipeError(), True),
        (RuntimeError(), False),
    ],
)
def test_cli_exception_classification(error: Exception, is_cli: bool):
    from snowflake.cli._app.telemetry import _is_cli_exception

    assert _is_cli_exception(error) == is_cli


def test_get_ci_integration_version_returns_value_when_set():
    """Test that integration version is returned when env var is set."""
    from snowflake.cli._app.telemetry import _get_ci_integration_version

    with mock.patch.dict(
        os.environ, {"SF_CICD_INTEGRATION_VERSION": "v2.0.2"}, clear=True
    ):
        assert _get_ci_integration_version() == "v2.0.2"


def test_get_ci_integration_version_returns_empty_when_unset():
    """Test that empty string is returned when env var is not set."""
    from snowflake.cli._app.telemetry import _get_ci_integration_version

    with mock.patch.dict(os.environ, {}, clear=True):
        assert _get_ci_integration_version() == ""


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_ci_integration_version_appears_in_telemetry(_, mock_conn, runner):
    """Test that integration version is included in telemetry payload."""
    with mock.patch.dict(
        os.environ,
        {"SF_GITHUB_ACTION": "true", "SF_CICD_INTEGRATION_VERSION": "v2.0.2"},
        clear=True,
    ):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    usage_command_event = (
        telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()
    )

    assert (
        usage_command_event["message"]["command_ci_environment"] == "SF_GITHUB_ACTION"
    )
    assert usage_command_event["message"]["command_ci_integration_version"] == "v2.0.2"


def test_get_ci_auth_type_returns_value_when_set():
    """Test that the auth type is returned when the env var is set."""
    from snowflake.cli._app.telemetry import _get_ci_auth_type

    with mock.patch.dict(os.environ, {"SF_CICD_AUTH_TYPE": "oidc"}, clear=True):
        assert _get_ci_auth_type() == "oidc"


def test_get_ci_auth_type_returns_empty_when_unset():
    """Test that empty string is returned when the env var is not set."""
    from snowflake.cli._app.telemetry import _get_ci_auth_type

    with mock.patch.dict(os.environ, {}, clear=True):
        assert _get_ci_auth_type() == ""


def test_get_ci_auth_type_is_normalized():
    """Test that the auth type is lower-cased and trimmed so integrations can
    emit any casing without fragmenting the telemetry value."""
    from snowflake.cli._app.telemetry import _get_ci_auth_type

    with mock.patch.dict(os.environ, {"SF_CICD_AUTH_TYPE": "  OIDC  "}, clear=True):
        assert _get_ci_auth_type() == "oidc"


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_ci_auth_type_appears_in_telemetry(_, mock_conn, runner):
    """Test that the auth type is included in the telemetry payload."""
    with mock.patch.dict(
        os.environ,
        {"SF_GITHUB_ACTION": "true", "SF_CICD_AUTH_TYPE": "oidc"},
        clear=True,
    ):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    telemetry = mock_conn.return_value._telemetry  # noqa: SLF001
    usage_command_event = (
        telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()
    )

    assert (
        usage_command_event["message"]["command_ci_environment"] == "SF_GITHUB_ACTION"
    )
    assert usage_command_event["message"]["command_ci_auth_type"] == "oidc"


@pytest.mark.parametrize(
    "authenticator, expected",
    [
        # Default password auth: the connector reports the DEFAULT_AUTHENTICATOR
        # token "SNOWFLAKE" even when the user configured nothing.
        ("SNOWFLAKE", "password"),
        ("snowflake", "password"),  # be defensive about casing
        ("SNOWFLAKE_JWT", "key_pair"),
        ("EXTERNALBROWSER", "externalbrowser"),
        ("OAUTH", "oauth"),
        # Authorization-code without a client id is the custom-client token.
        # The built-in local client is chosen in _classify_auth_type.
        ("OAUTH_AUTHORIZATION_CODE", "oauth_authorization_code"),
        ("OAUTH_CLIENT_CREDENTIALS", "oauth_client_credentials"),
        ("USERNAME_PASSWORD_MFA", "username_password_mfa"),
        ("PROGRAMMATIC_ACCESS_TOKEN", "programmatic_access_token"),
        ("PAT_WITH_EXTERNAL_SESSION", "programmatic_access_token"),
        ("WORKLOAD_IDENTITY", "workload_identity"),
        ("  SNOWFLAKE_JWT  ", "key_pair"),  # surrounding whitespace is trimmed
        # The Okta authenticator's token is the customer's Okta URL; it must be
        # collapsed so the endpoint never reaches telemetry.
        ("https://example.okta.com", "okta"),
        # Open vocabulary: an unknown token is recorded (lower-cased), not dropped.
        ("SOME_FUTURE_AUTH", "some_future_auth"),
        ("", ""),
        ("   ", ""),
    ],
)
def test_normalize_auth_type(authenticator, expected):
    """The resolved connector authenticator maps to a stable, URL-free token."""
    from snowflake.cli._app.telemetry import _normalize_auth_type

    assert _normalize_auth_type(authenticator) == expected


class _ConnectionWithoutAuthenticator:
    """Open connection that has no connector ``_authenticator`` attribute."""

    def __init__(self):
        self._telemetry = mock.Mock()
        self.schema = None
        self.role = None
        self.database = None
        self.warehouse = None
        self.host = "h"
        self.account = "acct"
        self.user = "usr"

    def close(self):
        return None

    @property
    def _authenticator(self):
        raise AssertionError(
            "command_auth_type must not read connection._authenticator"
        )


def _telemetry_messages(connection) -> list[dict]:
    messages = []
    for (
        call
    ) in connection._telemetry.try_add_log_to_batch.call_args_list:  # noqa: SLF001
        messages.append(call.args[0].to_dict()["message"])
    return messages


_JWT_CONFIG = """
[connections.default]
account = "acct"
user = "usr"
authenticator = "SNOWFLAKE_JWT"
"""

_OAUTH_CODE_CONFIG = """
[connections.default]
account = "acct"
user = "usr"
authenticator = "OAUTH_AUTHORIZATION_CODE"
"""

_EXTERNAL_SESSION_CONFIG = """
[connections.default]
account = "acct"
user = "usr"
authenticator = "SNOWFLAKE_JWT"
session_token = "sentinel-session-token"
master_token = "sentinel-master-token"
"""


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_auth_type_comes_from_resolved_parameters_not_connector(
    _, mock_conn, runner, config_file
):
    """A connection with no _authenticator still reports the resolved auth type."""
    mock_conn.return_value = _ConnectionWithoutAuthenticator()
    with config_file(_JWT_CONFIG) as config:
        with mock.patch.dict(os.environ, {}, clear=True):
            result = runner.invoke_with_config_file(config, ["connection", "test"])

    assert result.exit_code == 0, result.output
    connection = mock_conn.return_value
    messages = _telemetry_messages(connection)
    assert messages
    assert {message["command_auth_type"] for message in messages} == {"key_pair"}
    mock_conn.reset_mock()
    connection._telemetry.reset_mock()  # noqa: SLF001
    with config_file(_JWT_CONFIG) as config:
        with mock.patch.dict(os.environ, {}, clear=True):
            again = runner.invoke_with_config_file(config, ["connection", "test"])
    assert again.exit_code == 0, again.output
    # The cached connection is reused, so the second command does not connect
    # again and still reports the auth type recorded for that connection.
    mock_conn.assert_not_called()
    again_messages = _telemetry_messages(connection)
    assert again_messages
    assert {message["command_auth_type"] for message in again_messages} == {"key_pair"}


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_env_only_oauth_client_is_authorization_code_and_secrets_stay_out(
    _, mock_conn, runner, config_file
):
    """Client id and secret set only through the environment select the custom client.

    The sentinel values must not appear in the telemetry payload.
    """
    mock_conn.return_value = _ConnectionWithoutAuthenticator()
    sentinels = ("sentinel-client-id", "sentinel-client-secret")
    with config_file(_OAUTH_CODE_CONFIG) as config:
        with mock.patch.dict(
            os.environ,
            {
                "SNOWFLAKE_OAUTH_CLIENT_ID": sentinels[0],
                "SNOWFLAKE_OAUTH_CLIENT_SECRET": sentinels[1],
            },
            clear=True,
        ):
            result = runner.invoke_with_config_file(config, ["connection", "test"])

    assert result.exit_code == 0, result.output
    messages = _telemetry_messages(mock_conn.return_value)
    assert messages
    assert {message["command_auth_type"] for message in messages} == {
        "oauth_authorization_code"
    }
    serialized = repr(messages)
    for sentinel in sentinels:
        assert sentinel not in serialized


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_external_session_tokens_report_external_session_and_stay_out_of_payload(
    _, mock_conn, runner, config_file
):
    """session_token plus master_token wins over a configured authenticator."""
    mock_conn.return_value = _ConnectionWithoutAuthenticator()
    sentinels = ("sentinel-session-token", "sentinel-master-token")
    with config_file(_EXTERNAL_SESSION_CONFIG) as config:
        with mock.patch.dict(os.environ, {}, clear=True):
            result = runner.invoke_with_config_file(config, ["connection", "test"])

    assert result.exit_code == 0, result.output
    messages = _telemetry_messages(mock_conn.return_value)
    assert messages
    assert {message["command_auth_type"] for message in messages} == {
        "external_session"
    }
    serialized = repr(messages)
    for sentinel in sentinels:
        assert sentinel not in serialized


def test_sf_gitlab_component_takes_priority_over_gitlab_ci():
    """Test that SF_GITLAB_COMPONENT is detected before generic GITLAB_CI."""
    from snowflake.cli._app.telemetry import _get_ci_environment_type

    with mock.patch.dict(
        os.environ,
        {"SF_GITLAB_COMPONENT": "true", "GITLAB_CI": "true"},
        clear=True,
    ):
        assert _get_ci_environment_type() == "SF_GITLAB_COMPONENT"


def test_sf_ado_extension_takes_priority_over_azure_devops():
    """Test that SF_ADO_EXTENSION is detected before generic AZURE_DEVOPS."""
    from snowflake.cli._app.telemetry import _get_ci_environment_type

    with mock.patch.dict(
        os.environ,
        {"SF_ADO_EXTENSION": "true", "TF_BUILD": "true"},
        clear=True,
    ):
        assert _get_ci_environment_type() == "SF_ADO_EXTENSION"


@mock.patch("uuid.uuid4")
def test_flags_from_parent_contexts_are_captured(mock_uuid4, mock_connect, runner):
    mock_uuid4.return_value = uuid.UUID("8a2225b3800c4017a4a9eab941db58fa")

    result = runner.invoke(
        ["dbt", "execute", "--run-async", "pipeline_name", "run", "--debug"]
    )

    assert result.exit_code == 0, result.output

    telemetry = mock_connect.return_value._telemetry  # noqa: SLF001
    usage_command_event = (
        telemetry.try_add_log_to_batch.call_args_list[0].args[0].to_dict()
    )

    command_flags = usage_command_event["message"]["command_flags"]
    assert (
        "run_async" in command_flags
    ), f"run_async flag should be captured in telemetry. Found flags: {command_flags}"


_INTERACTIVE_CONFIG = """
[connections.default]
account = "acct"
user = "usr"
authenticator = "externalbrowser"
"""

_SILENT_CONFIG = """
[connections.default]
account = "acct"
user = "usr"
password = "pwd"
"""


@mock.patch("snowflake.connector.connect")
def test_telemetry_does_not_open_connection_for_interactive_authenticator(
    mock_connect, runner, config_file
):
    """Telemetry must never be the reason the CLI authenticates.

    `connection list` needs no Snowflake connection, but telemetry used to reach
    for one to get the connector's telemetry channel -- which for externalbrowser
    launched a browser on every command and hung headless/CI runs.
    """
    for authenticator in (
        "externalbrowser",
        "oauth_authorization_code",
        "username_password_mfa",
    ):
        mock_connect.reset_mock()
        config_text = _INTERACTIVE_CONFIG.replace("externalbrowser", authenticator)
        with config_file(config_text) as config:
            with mock.patch.dict(os.environ, {}, clear=True):
                result = runner.invoke_with_config_file(config, ["connection", "list"])

        assert result.exit_code == 0, result.output
        mock_connect.assert_not_called()


@mock.patch("snowflake.connector.connect")
def test_telemetry_does_not_open_connection_for_env_var_authenticator(
    mock_connect, runner, config_file
):
    """SNOWFLAKE_AUTHENTICATOR needs its own check because the CLI's config layer
    merges only SNOWFLAKE_CONNECTIONS_* into the connection context -- and setting
    it with no authenticator in config.toml is the common CI setup."""
    with config_file(_SILENT_CONFIG) as config:
        with mock.patch.dict(
            os.environ, {"SNOWFLAKE_AUTHENTICATOR": "externalbrowser"}, clear=True
        ):
            result = runner.invoke_with_config_file(config, ["connection", "list"])

    assert result.exit_code == 0, result.output
    mock_connect.assert_not_called()


@mock.patch("snowflake.connector.connect")
def test_telemetry_prefers_config_authenticator_over_env_var(
    mock_connect, runner, config_file
):
    """The env var is a fallback, not an override -- same order as
    `connect_to_snowflake`, whose step (2) applies SUPPORTED_ENV_OVERRIDES only to
    keys config and flags left unset.

    Resolving it the other way round let a leftover silent SNOWFLAKE_AUTHENTICATOR
    mask an interactive config.toml, so telemetry opened a connection that still
    went through SSO -- exactly the browser this change exists to prevent.
    """
    with config_file(_INTERACTIVE_CONFIG) as config:
        with mock.patch.dict(
            os.environ, {"SNOWFLAKE_AUTHENTICATOR": "SNOWFLAKE"}, clear=True
        ):
            result = runner.invoke_with_config_file(config, ["connection", "list"])

    assert result.exit_code == 0, result.output
    mock_connect.assert_not_called()


@mock.patch("snowflake.connector.connect")
def test_telemetry_may_open_connection_for_silent_authenticator(
    mock_connect, runner, config_file
):
    """The gate is on interaction, not on connecting: where logging in is silent
    telemetry still opens a connection, so coverage of commands that never
    connect is unchanged."""
    with config_file(_SILENT_CONFIG) as config:
        with mock.patch.dict(os.environ, {}, clear=True):
            result = runner.invoke_with_config_file(config, ["connection", "list"])

    assert result.exit_code == 0, result.output
    mock_connect.assert_called()


@mock.patch("snowflake.connector.connect")
def test_get_auth_type_does_not_open_connection(mock_connect):
    """_get_auth_type reads an already-open connection or nothing at all."""
    from snowflake.cli._app.telemetry import _get_auth_type

    assert _get_auth_type() == ""
    mock_connect.assert_not_called()


# Resolved connect() parameters after config, flags and SNOWFLAKE_* env merge.
# Expected tokens are literals from SNOW-4389588, not recomputed by the test.
_AUTH_TYPE_CASES = [
    ({"authenticator": "OAUTH"}, "oauth"),
    ({"authenticator": "oauth"}, "oauth"),
    ({"authenticator": "OAUTH", "oauth_client_id": "custom-client"}, "oauth"),
    ({"authenticator": "OAUTH_AUTHORIZATION_CODE"}, "oauth_local_application"),
    (
        {
            "authenticator": "OAUTH_AUTHORIZATION_CODE",
            "oauth_client_id": "",
            "oauth_client_secret": None,
        },
        "oauth_local_application",
    ),
    (
        {
            "authenticator": "OAUTH_AUTHORIZATION_CODE",
            "oauth_client_id": "LOCAL_APPLICATION",
        },
        "oauth_local_application",
    ),
    (
        {
            "authenticator": "OAUTH_AUTHORIZATION_CODE",
            "oauth_client_id": "LOCAL_APPLICATION",
            "oauth_client_secret": "LOCAL_APPLICATION",
        },
        "oauth_local_application",
    ),
    (
        {
            "authenticator": "OAUTH_AUTHORIZATION_CODE",
            "oauth_client_id": "LOCAL_APPLICATION",
            "oauth_client_secret": "",
        },
        "oauth_local_application",
    ),
    (
        {
            "authenticator": "OAUTH_AUTHORIZATION_CODE",
            "oauth_client_id": "other-client",
        },
        "oauth_authorization_code",
    ),
    (
        {
            "authenticator": "OAUTH_AUTHORIZATION_CODE",
            "oauth_client_secret": "other-secret",
        },
        "oauth_authorization_code",
    ),
    (
        {
            "authenticator": "OAUTH_AUTHORIZATION_CODE",
            "oauth_client_id": "LOCAL_APPLICATION",
            "oauth_client_secret": "other-secret",
        },
        "oauth_authorization_code",
    ),
    (
        {
            "authenticator": "OAUTH_AUTHORIZATION_CODE",
            "oauth_client_id": "other-client",
            "oauth_client_secret": "LOCAL_APPLICATION",
        },
        "oauth_authorization_code",
    ),
    (
        {
            "authenticator": "OAUTH_AUTHORIZATION_CODE",
            "oauth_client_id": "local_application",
        },
        "oauth_authorization_code",
    ),
    (
        {"authenticator": "OAUTH_CLIENT_CREDENTIALS"},
        "oauth_client_credentials",
    ),
    (
        {"authenticator": "oauth_client_credentials"},
        "oauth_client_credentials",
    ),
    (
        {"session_token": "session-value", "master_token": "master-value"},
        "external_session",
    ),
    (
        {
            "authenticator": "SNOWFLAKE_JWT",
            "session_token": "session-value",
            "master_token": "master-value",
        },
        "external_session",
    ),
    (
        {
            "authenticator": "PAT_WITH_EXTERNAL_SESSION",
            "session_token": "session-value",
            "master_token": "master-value",
        },
        "external_session",
    ),
    (
        {"session_token": "session-value"},
        "password",
    ),
    (
        {"session_token": "   ", "master_token": "master-value"},
        "password",
    ),
    ({"authenticator": "PAT_WITH_EXTERNAL_SESSION"}, "programmatic_access_token"),
    ({"authenticator": "SNOWFLAKE"}, "password"),
    ({}, "password"),
    ({"authenticator": "SNOWFLAKE_JWT"}, "key_pair"),
    ({"authenticator": "  SNOWFLAKE_JWT  "}, "key_pair"),
    ({"authenticator": "EXTERNALBROWSER"}, "externalbrowser"),
    ({"authenticator": "USERNAME_PASSWORD_MFA"}, "username_password_mfa"),
    ({"authenticator": "PROGRAMMATIC_ACCESS_TOKEN"}, "programmatic_access_token"),
    ({"authenticator": "WORKLOAD_IDENTITY"}, "workload_identity"),
    ({"authenticator": "https://example.okta.com"}, "okta"),
    ({"authenticator": "SOME_FUTURE_AUTH"}, "some_future_auth"),
]


@pytest.mark.parametrize("parameters, expected", _AUTH_TYPE_CASES)
def test_resolved_connection_parameters_set_command_auth_type(parameters, expected):
    """command_auth_type comes from the parameters passed to connect()."""
    from snowflake.cli._app.telemetry import (
        _auth_type_by_connection_id,
        _classify_auth_type,
        _get_auth_type,
        bind_connection_auth_type,
    )
    from snowflake.connector.connection import SnowflakeConnection

    assert _classify_auth_type(parameters) == expected

    connection = SnowflakeConnection.__new__(SnowflakeConnection)
    try:
        bind_connection_auth_type(connection, parameters)
        context = mock.Mock()
        context.connection_if_open = connection
        with mock.patch(
            "snowflake.cli._app.telemetry.get_cli_context", return_value=context
        ):
            assert _get_auth_type() == expected
        # An uninitialized connection has no _authenticator on connector 4 or 5.
        assert not hasattr(connection, "_authenticator")
    finally:
        _auth_type_by_connection_id.pop(id(connection), None)


class _LoginCursor:
    def __init__(self, user: str):
        self._user = user

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _query, *_args, **_kwargs):
        return self

    def fetchone(self):
        return (self._user,)


class _LoginConnection:
    def __init__(self, user: str):
        self._user = user
        self._telemetry = mock.Mock()
        self.closed = False

    def cursor(self):
        return _LoginCursor(self._user)

    def close(self):
        self.closed = True


def test_successful_auth_login_reports_local_oauth(runner, config_file, monkeypatch):
    """A finished snow auth login records oauth_local_application on its events."""
    users = ["JDOE", "JDOE"]
    connections: list[_LoginConnection] = []

    def connect(**_kwargs):
        connection = _LoginConnection(users.pop(0))
        connections.append(connection)
        return connection

    monkeypatch.setattr("snowflake.connector.connect", connect)
    with config_file(_SILENT_CONFIG) as config:
        with with_feature_flags({FeatureFlag.ENABLE_AUTH_COMMANDS: True}):
            result = runner.invoke_with_config_file(
                config, ["auth", "login", "myorg-myacct"]
            )

    assert result.exit_code == 0, result.output
    assert len(connections) == 2
    discovery, caching = connections
    assert discovery._telemetry.try_add_log_to_batch.call_count == 0  # noqa: SLF001
    messages = _telemetry_messages(caching)
    assert messages
    assert {message["command_auth_type"] for message in messages} == {
        "oauth_local_application"
    }
    assert all(message["command"] == ["auth", "login"] for message in messages)
    assert discovery.closed
    assert caching.closed


def test_failed_auth_login_does_not_report_local_oauth(
    runner, config_file, monkeypatch
):
    """A rejected snow auth login does not record oauth_local_application."""
    from snowflake.cli._app.telemetry import CLITelemetryClient
    from snowflake.connector.errors import Error as SnowflakeError

    recorded: list[str] = []
    original = CLITelemetryClient.generate_telemetry_data_dict

    def spy(payload, command_info=None):
        data = original(payload, command_info=command_info)
        recorded.append(data.get("command_auth_type", ""))
        return data

    def connect(**_kwargs):
        raise SnowflakeError("The account refused the sign-in.")

    monkeypatch.setattr("snowflake.connector.connect", connect)
    monkeypatch.setattr(
        CLITelemetryClient, "generate_telemetry_data_dict", staticmethod(spy)
    )
    with config_file(_SILENT_CONFIG) as config:
        with with_feature_flags({FeatureFlag.ENABLE_AUTH_COMMANDS: True}):
            result = runner.invoke_with_config_file(
                config, ["auth", "login", "myorg-myacct"]
            )

    assert result.exit_code != 0, result.output
    assert recorded
    assert set(recorded) == {""}


class TestAppYmlRuntimeTelemetry:
    """``snow app`` result events carry the app.yml runtime fields."""

    @mock.patch("snowflake.connector.connect")
    def test_streamlit_app_yml_v2(self, mock_conn, project_directory, runner):
        with project_directory("snowflake_app_yml_streamlit"):
            runner.invoke(["app", "bundle"], catch_exceptions=False)

        events = _get_telemetry_events(mock_conn)
        _assert_executing_event_has_no_app_flow(events)
        results = [
            e["message"]
            for e in events
            if e["message"]["type"] == "result_executing_command"
        ]
        assert results
        for message in results:
            assert message["app_flow"] == "snowflake_app"
            assert message["app_runtime"] == "python"
            assert message["is_streamlit"] is True
            for dropped in (
                "app_runtime_source",
                "app_template_type",
                "app_template_version",
                "app_created_by",
                "app_template",
            ):
                assert dropped not in message
        for event in events:
            if event["message"]["type"] == "executing_command":
                assert "app_runtime" not in event["message"]

    @mock.patch("snowflake.connector.connect")
    def test_nextjs_versionless_app_yml_with_snowflake_yml(
        self, mock_conn, project_directory, runner
    ):
        with project_directory("snowflake_app_nextjs_versionless"):
            runner.invoke(["app", "bundle"], catch_exceptions=False)

        events = _get_telemetry_events(mock_conn)
        results = [
            e["message"]
            for e in events
            if e["message"]["type"] == "result_executing_command"
        ]
        assert results
        for message in results:
            assert message["app_flow"] == "snowflake_app"
            assert message["app_runtime"] == "node"
            assert message["is_streamlit"] is False
            assert "app_template" not in message

    @mock.patch("snowflake.connector.connect")
    def test_absent_without_app_yml(self, mock_conn, project_directory, runner):
        with project_directory("snowflake_app_v2"):
            runner.invoke(["app", "bundle"], catch_exceptions=False)

        for event in _get_telemetry_events(mock_conn):
            assert not [
                k for k in event["message"] if k.startswith("app_") and k != "app_flow"
            ]

    @mock.patch("snowflake.connector.connect")
    def test_absent_for_native_app(self, mock_conn, project_directory, runner):
        with project_directory("napp_post_deploy_missing_file"):
            runner.invoke(["app", "bundle"], catch_exceptions=False)

        for event in _get_telemetry_events(mock_conn):
            assert not [
                k for k in event["message"] if k.startswith("app_") and k != "app_flow"
            ]


def test_command_info_falls_back_without_click_context():
    from concurrent.futures import ThreadPoolExecutor

    from snowflake.cli._app.telemetry import command_info

    with ThreadPoolExecutor(max_workers=1) as pool:
        assert pool.submit(command_info).result() == "SNOWCLI"


def test_command_info_uses_click_command_path():
    import click
    from snowflake.cli._app.telemetry import command_info

    @click.group()
    def snow():
        pass

    @snow.command()
    def stage():
        pass

    parent = click.Context(snow, info_name="snow")
    ctx = click.Context(stage, info_name="stage", parent=parent)
    with ctx:
        assert command_info() == "SNOWCLI.STAGE"
