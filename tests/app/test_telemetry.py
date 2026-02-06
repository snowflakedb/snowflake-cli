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

import os
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
from snowflake.cli.api.feature_flags import BooleanFlag, FeatureFlagMixin
from snowflake.connector import ProgrammingError
from snowflake.connector.version import VERSION as DRIVER_VERSION

from tests_common.feature_flag_utils import with_feature_flags


class _TestFlags(FeatureFlagMixin):
    FOO = BooleanFlag("FOO", False)


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
        usage_command_event = (
            mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
                0
            ]
            .args[0]
            .to_dict()
        )

        del usage_command_event["message"][
            "command_ci_environment"
        ]  # to avoid side effect from CI
        del usage_command_event["message"][
            "command_agent_environment"
        ]  # to avoid side effect from agent environment
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
        usage_command_event = (
            mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
                0
            ]
            .args[0]
            .to_dict()
        )

        del usage_command_event["message"][
            "command_ci_environment"
        ]  # to avoid side effect from CI
        del usage_command_event["message"][
            "command_agent_environment"
        ]  # to avoid side effect from agent environment

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
    usage_command_event = (
        mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            0
        ]
        .args[0]
        .to_dict()
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
    usage_command_event = (
        mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            0
        ]
        .args[0]
        .to_dict()
    )

    assert usage_command_event["message"]["command_ci_environment"] == "UNKNOWN_CI"


def test_is_interactive_terminal_returns_true_when_tty():
    """Test _is_interactive_terminal returns True when both stdin and stdout are TTYs."""
    from snowflake.cli._app.telemetry import _is_interactive_terminal

    with mock.patch("sys.stdin") as mock_stdin, mock.patch("sys.stdout") as mock_stdout:
        mock_stdin.isatty.return_value = True
        mock_stdout.isatty.return_value = True
        assert _is_interactive_terminal() is True


def test_is_interactive_terminal_returns_false_when_not_tty():
    """Test _is_interactive_terminal returns False when stdin or stdout is not a TTY."""
    from snowflake.cli._app.telemetry import _is_interactive_terminal

    # stdin is not a TTY
    with mock.patch("sys.stdin") as mock_stdin, mock.patch("sys.stdout") as mock_stdout:
        mock_stdin.isatty.return_value = False
        mock_stdout.isatty.return_value = True
        assert _is_interactive_terminal() is False

    # stdout is not a TTY
    with mock.patch("sys.stdin") as mock_stdin, mock.patch("sys.stdout") as mock_stdout:
        mock_stdin.isatty.return_value = True
        mock_stdout.isatty.return_value = False
        assert _is_interactive_terminal() is False


def test_is_interactive_terminal_returns_false_on_exception():
    """Test _is_interactive_terminal returns False when isatty() raises an exception."""
    from snowflake.cli._app.telemetry import _is_interactive_terminal

    with mock.patch("sys.stdin") as mock_stdin:
        mock_stdin.isatty.side_effect = Exception("No TTY available")
        assert _is_interactive_terminal() is False


def test_get_ci_environment_type_returns_local_for_interactive_terminal():
    """Test that LOCAL is returned when running in an interactive terminal."""
    from snowflake.cli._app.telemetry import _get_ci_environment_type

    with mock.patch.dict(os.environ, {}, clear=True):
        with mock.patch(
            "snowflake.cli._app.telemetry._is_interactive_terminal", return_value=True
        ):
            assert _get_ci_environment_type() == "LOCAL"


def test_get_ci_environment_type_returns_unknown_for_non_interactive_non_ci():
    """Test that UNKNOWN is returned when not in CI and not in an interactive terminal."""
    from snowflake.cli._app.telemetry import _get_ci_environment_type

    with mock.patch.dict(os.environ, {}, clear=True):
        with mock.patch(
            "snowflake.cli._app.telemetry._is_interactive_terminal", return_value=False
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
        ("CURSOR_AGENT", "1", "CURSOR"),
        ("GEMINI_CLI", "1", "GEMINI_CLI"),
        ("CLAUDECODE", "1", "CLAUDE_CODE"),
        ("CODEX_API_KEY", "key123", "CODEX"),
        ("OPENCODE", "1", "OPENCODE"),
    ],
)
def test_detect_agent_environment_returns_correct_agent(
    env_var, env_value, expected_agent
):
    """Test that the correct agent is detected based on environment variables."""
    from snowflake.cli._app.telemetry import _detect_agent_environment

    with mock.patch.dict(os.environ, {env_var: env_value}, clear=True):
        assert _detect_agent_environment() == expected_agent


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
            "snowflake.cli._app.telemetry._is_interactive_terminal", return_value=False
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
    result_command_event = (
        mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            1
        ]
        .args[0]
        .to_dict()
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
@mock.patch("uuid.uuid4")
@mock.patch("snowflake.cli._plugins.streamlit.commands.StreamlitManager")
def test_failing_executing_command_sends_telemetry_data(
    _, mock_uuid4, mock_conn, project_directory, runner
):
    mock_uuid4.return_value = uuid.UUID("8a2225b3800c4017a4a9eab941db58fa")
    with project_directory("napp_post_deploy_missing_file"):
        runner.invoke(["app", "run"], catch_exceptions=False)

    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    result_command_event = (
        mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            1
        ]
        .args[0]
        .to_dict()
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


@mock.patch("uuid.uuid4")
def test_flags_from_parent_contexts_are_captured(mock_uuid4, mock_connect, runner):
    mock_uuid4.return_value = uuid.UUID("8a2225b3800c4017a4a9eab941db58fa")

    result = runner.invoke(
        ["dbt", "execute", "--run-async", "pipeline_name", "run", "--debug"]
    )

    assert result.exit_code == 0, result.output

    usage_command_event = (
        mock_connect.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            0
        ]
        .args[0]
        .to_dict()
    )

    command_flags = usage_command_event["message"]["command_flags"]
    assert (
        "run_async" in command_flags
    ), f"run_async flag should be captured in telemetry. Found flags: {command_flags}"
