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
    usage_command_event = (
        mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            0
        ]
        .args[0]
        .to_dict()
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
    usage_command_event = (
        mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            0
        ]
        .args[0]
        .to_dict()
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
        ("OAUTH_AUTHORIZATION_CODE", "oauth"),
        ("OAUTH_CLIENT_CREDENTIALS", "oauth"),
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


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_auth_type_appears_in_telemetry(_, mock_conn, runner):
    """The auth type resolved on the live connection is recorded for any command,
    independent of the CI/CD environment."""
    mock_conn.return_value._authenticator = "SNOWFLAKE_JWT"  # noqa: SLF001
    with mock.patch.dict(os.environ, {}, clear=True):
        result = runner.invoke(["connection", "test"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    usage_command_event = (
        mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list[  # noqa: SLF001
            0
        ]
        .args[0]
        .to_dict()
    )

    assert usage_command_event["message"]["command_auth_type"] == "key_pair"


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
