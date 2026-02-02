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

import os
import platform
import sys
from enum import Enum, unique
from typing import Any, Dict, Optional, Union

import click
import typer
from snowflake.cli import __about__
from snowflake.cli._app.cli_app import INTERNAL_CLI_FLAGS
from snowflake.cli._app.constants import PARAM_APPLICATION_NAME
from snowflake.cli.api.cli_global_context import (
    _CliGlobalContextAccess,
    get_cli_context,
)
from snowflake.cli.api.commands.execution_metadata import ExecutionMetadata
from snowflake.cli.api.config import get_feature_flags_section
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.utils.error_handling import ignore_exceptions
from snowflake.connector import ProgrammingError
from snowflake.connector.telemetry import (
    TelemetryData,
    TelemetryField,
)
from snowflake.connector.time_util import get_time_millis
from typer import Context


@unique
class CLITelemetryField(Enum):
    # Basic information
    SOURCE = "source"
    VERSION_CLI = "version_cli"
    VERSION_PYTHON = "version_python"
    VERSION_OS = "version_os"
    INSTALLATION_SOURCE = "installation_source"
    # Command execution context
    COMMAND = "command"
    COMMAND_GROUP = "command_group"
    COMMAND_FLAGS = "command_flags"
    COMMAND_EXECUTION_ID = "command_execution_id"
    COMMAND_RESULT_STATUS = "command_result_status"
    COMMAND_OUTPUT_TYPE = "command_output_type"
    COMMAND_EXECUTION_TIME = "command_execution_time"
    COMMAND_CI_ENVIRONMENT = "command_ci_environment"
    COMMAND_AGENT_ENVIRONMENT = "command_agent_environment"
    # Configuration
    CONFIG_FEATURE_FLAGS = "config_feature_flags"
    CONFIG_PROVIDER_TYPE = "config_provider_type"
    CONFIG_SOURCES_USED = "config_sources_used"
    CONFIG_SOURCE_WINS = "config_source_wins"
    CONFIG_TOTAL_KEYS_RESOLVED = "config_total_keys_resolved"
    CONFIG_KEYS_WITH_OVERRIDES = "config_keys_with_overrides"
    # Metrics
    COUNTERS = "counters"
    SPANS = "spans"
    COMPLETED_SPANS = "completed_spans"
    NUM_SPANS_PAST_DEPTH_LIMIT = "num_spans_past_depth_limit"
    NUM_SPANS_PAST_TOTAL_LIMIT = "num_spans_past_total_limit"
    # Information
    EVENT = "event"
    ERROR_MSG = "error_msg"
    ERROR_TYPE = "error_type"
    ERROR_CODE = "error_code"
    ERROR_CAUSE = "error_cause"
    SQL_STATE = "sql_state"
    IS_CLI_EXCEPTION = "is_cli_exception"
    # Project context
    PROJECT_DEFINITION_VERSION = "project_definition_version"
    MODE = "mode"


class TelemetryEvent(Enum):
    CMD_EXECUTION = "executing_command"
    CMD_EXECUTION_ERROR = "error_executing_command"
    CMD_EXECUTION_RESULT = "result_executing_command"


TelemetryDict = Dict[Union[CLITelemetryField, TelemetryField], Any]


def _is_cli_exception(exception: Exception) -> bool:
    return isinstance(
        exception,
        (
            click.ClickException,
            typer.Exit,
            typer.Abort,
            BrokenPipeError,
            KeyboardInterrupt,
        ),
    )


def _get_additional_exception_information(exception: Exception) -> TelemetryDict:
    """
    Attach the errno and sqlstate if the exception or the
    cause of the exception is a ProgrammingError
    """
    additional_info = {}

    if isinstance(exception, ProgrammingError):
        additional_info[CLITelemetryField.ERROR_CODE] = exception.errno
        additional_info[CLITelemetryField.SQL_STATE] = exception.sqlstate

    if exception.__cause__:
        cause = exception.__cause__
        additional_info[CLITelemetryField.ERROR_CAUSE] = type(cause).__name__

        if isinstance(cause, ProgrammingError):
            if not additional_info.get(CLITelemetryField.ERROR_CODE):
                additional_info[CLITelemetryField.ERROR_CODE] = cause.errno
            if not additional_info.get(CLITelemetryField.SQL_STATE):
                additional_info[CLITelemetryField.SQL_STATE] = cause.sqlstate

    return additional_info


def _get_command_metrics() -> TelemetryDict:
    cli_context = get_cli_context()

    return {
        CLITelemetryField.COUNTERS: cli_context.metrics.counters,
        CLITelemetryField.SPANS: {
            CLITelemetryField.COMPLETED_SPANS.value: cli_context.metrics.completed_spans,
            CLITelemetryField.NUM_SPANS_PAST_DEPTH_LIMIT.value: cli_context.metrics.num_spans_past_depth_limit,
            CLITelemetryField.NUM_SPANS_PAST_TOTAL_LIMIT.value: cli_context.metrics.num_spans_past_total_limit,
        },
    }


def _find_command_info() -> TelemetryDict:
    ctx = click.get_current_context()
    command_path = ctx.command_path.split(" ")[1:]

    command_flags = {}
    format_value = None
    current_ctx: Optional[Context] = ctx
    while current_ctx:
        for flag, flag_value in current_ctx.params.items():
            if (
                flag_value
                and flag not in command_flags
                and flag not in INTERNAL_CLI_FLAGS
            ):
                command_flags[flag] = current_ctx.get_parameter_source(flag).name  # type: ignore[attr-defined]
        if format_value is None and "format" in current_ctx.params:
            format_value = current_ctx.params["format"]
        current_ctx = current_ctx.parent

    if format_value is None:
        format_value = OutputFormat.TABLE

    return {
        CLITelemetryField.COMMAND: command_path,
        CLITelemetryField.COMMAND_GROUP: command_path[0],
        CLITelemetryField.COMMAND_FLAGS: command_flags,
        CLITelemetryField.COMMAND_OUTPUT_TYPE: format_value.value,
        CLITelemetryField.PROJECT_DEFINITION_VERSION: str(_get_definition_version()),
        CLITelemetryField.MODE: _get_cli_running_mode(),
    }


def _get_cli_running_mode() -> str:
    try:
        if get_cli_context().is_repl:
            return "repl"
    except Exception:
        pass
    return "cmd"


def _get_definition_version() -> str | None:
    try:
        cli_context = get_cli_context()
        if cli_context.project_definition:
            return cli_context.project_definition.definition_version
    except Exception:
        # Don't let an invalid project definition file break telemetry
        # (especially for commands that don't normally load it)
        pass
    return None


def _is_interactive_terminal() -> bool:
    """Check if stdin and stdout are connected to a TTY."""
    try:
        return sys.stdin.isatty() and sys.stdout.isatty()
    except Exception:
        return False


def _is_env_truthy(name: str) -> bool:
    """Check if an environment variable has a truthy value."""
    return os.environ.get(name, "").lower() in ("yes", "true", "1", "on")


def _get_ci_environment_type() -> str:
    """Detect CI/CD environment type based on environment variables."""
    if "SF_GITHUB_ACTION" in os.environ:
        return "SF_GITHUB_ACTION"
    if "GITHUB_ACTIONS" in os.environ:
        return "GITHUB_ACTIONS"
    if "GITLAB_CI" in os.environ:
        return "GITLAB_CI"
    if "CIRCLECI" in os.environ:
        return "CIRCLECI"
    if "JENKINS_URL" in os.environ or "HUDSON_URL" in os.environ:
        return "JENKINS"
    if "TF_BUILD" in os.environ:
        return "AZURE_DEVOPS"
    if "BITBUCKET_BUILD_NUMBER" in os.environ:
        return "BITBUCKET_PIPELINES"
    if "CODEBUILD_BUILD_ID" in os.environ:
        return "AWS_CODEBUILD"
    if "TEAMCITY_VERSION" in os.environ:
        return "TEAMCITY"
    if "BUILDKITE" in os.environ:
        return "BUILDKITE"
    if "CF_BUILD_ID" in os.environ:
        return "CODEFRESH"
    if "TRAVIS" in os.environ:
        return "TRAVIS_CI"
    if _is_env_truthy("CI"):
        return "UNKNOWN_CI"
    if _is_interactive_terminal():
        return "LOCAL"
    return "UNKNOWN"


def _detect_agent_environment() -> str:
    """Detect AI coding agent based on environment variables."""
    if "CORTEX_SESSION_ID" in os.environ:
        return "CORTEX"
    if _is_env_truthy("CURSOR_AGENT"):
        return "CURSOR"
    if _is_env_truthy("CLAUDECODE"):
        return "CLAUDE_CODE"
    if _is_env_truthy("GEMINI_CLI"):
        return "GEMINI_CLI"
    if _is_env_truthy("OPENCODE"):
        return "OPENCODE"
    if "CODEX_API_KEY" in os.environ:
        return "CODEX"
    return "UNKNOWN"


def command_info() -> str:
    info = _find_command_info()
    command = ".".join(info[CLITelemetryField.COMMAND])
    return f"{PARAM_APPLICATION_NAME}.{command}".upper()


def python_version() -> str:
    py_ver = sys.version_info
    return f"{py_ver.major}.{py_ver.minor}.{py_ver.micro}"


def _get_config_telemetry() -> TelemetryDict:
    """Get configuration resolution telemetry data."""
    try:
        from snowflake.cli.api.config_provider import (
            AlternativeConfigProvider,
            get_config_provider_singleton,
        )

        provider = get_config_provider_singleton()

        # Identify which config provider is being used
        provider_type = (
            "ng" if isinstance(provider, AlternativeConfigProvider) else "legacy"
        )

        result: TelemetryDict = {CLITelemetryField.CONFIG_PROVIDER_TYPE: provider_type}

        # Get detailed telemetry if using ng config
        if isinstance(provider, AlternativeConfigProvider):
            payload = provider.resolution_summary

            # Map payload keys to telemetry fields
            if payload:
                if "config_sources_used" in payload:
                    result[CLITelemetryField.CONFIG_SOURCES_USED] = payload[
                        "config_sources_used"
                    ]
                if "config_source_wins" in payload:
                    result[CLITelemetryField.CONFIG_SOURCE_WINS] = payload[
                        "config_source_wins"
                    ]
                if "config_total_keys_resolved" in payload:
                    result[CLITelemetryField.CONFIG_TOTAL_KEYS_RESOLVED] = payload[
                        "config_total_keys_resolved"
                    ]
                if "config_keys_with_overrides" in payload:
                    result[CLITelemetryField.CONFIG_KEYS_WITH_OVERRIDES] = payload[
                        "config_keys_with_overrides"
                    ]

        return result
    except Exception:
        return {}


class CLITelemetryClient:
    @property
    def _ctx(self) -> _CliGlobalContextAccess:
        return get_cli_context()

    @staticmethod
    def generate_telemetry_data_dict(
        telemetry_payload: TelemetryDict,
    ) -> Dict[str, Any]:
        data = {
            CLITelemetryField.SOURCE: PARAM_APPLICATION_NAME,
            CLITelemetryField.INSTALLATION_SOURCE: __about__.INSTALLATION_SOURCE.value,
            CLITelemetryField.VERSION_CLI: __about__.VERSION,
            CLITelemetryField.VERSION_OS: platform.platform(),
            CLITelemetryField.VERSION_PYTHON: python_version(),
            CLITelemetryField.COMMAND_CI_ENVIRONMENT: _get_ci_environment_type(),
            CLITelemetryField.COMMAND_AGENT_ENVIRONMENT: _detect_agent_environment(),
            CLITelemetryField.CONFIG_FEATURE_FLAGS: {
                k: str(v) for k, v in get_feature_flags_section().items()
            },
            **_find_command_info(),
            **_get_config_telemetry(),
            **telemetry_payload,
        }
        # To map Enum to string, so we don't have to use .value every time
        return {getattr(k, "value", k): v for k, v in data.items()}  # type: ignore[arg-type, misc]

    @property
    def _telemetry(self):
        return self._ctx.connection._telemetry  # noqa

    def send(self, payload: TelemetryDict):
        if self._telemetry:
            message = self.generate_telemetry_data_dict(payload)
            telemetry_data = TelemetryData.from_telemetry_data_dict(
                from_dict=message, timestamp=get_time_millis()
            )
            self._telemetry.try_add_log_to_batch(telemetry_data)

    def flush(self):
        self._telemetry.send_batch()


_telemetry = CLITelemetryClient()


@ignore_exceptions()
def log_command_usage(execution: ExecutionMetadata):
    _telemetry.send(
        {
            TelemetryField.KEY_TYPE: TelemetryEvent.CMD_EXECUTION.value,
            CLITelemetryField.COMMAND_EXECUTION_ID: execution.execution_id,
        }
    )


@ignore_exceptions()
def log_command_result(execution: ExecutionMetadata):
    _telemetry.send(
        {
            TelemetryField.KEY_TYPE: TelemetryEvent.CMD_EXECUTION_RESULT.value,
            CLITelemetryField.COMMAND_EXECUTION_ID: execution.execution_id,
            CLITelemetryField.COMMAND_RESULT_STATUS: execution.status.value,
            CLITelemetryField.COMMAND_EXECUTION_TIME: execution.get_duration(),
            **_get_command_metrics(),
        }
    )


@ignore_exceptions()
def log_command_execution_error(exception: Exception, execution: ExecutionMetadata):
    exception_type: str = type(exception).__name__
    is_cli_exception: bool = _is_cli_exception(exception)
    _telemetry.send(
        {
            TelemetryField.KEY_TYPE: TelemetryEvent.CMD_EXECUTION_ERROR.value,
            CLITelemetryField.COMMAND_EXECUTION_ID: execution.execution_id,
            CLITelemetryField.ERROR_TYPE: exception_type,
            CLITelemetryField.IS_CLI_EXCEPTION: is_cli_exception,
            CLITelemetryField.COMMAND_EXECUTION_TIME: execution.get_duration(),
            **_get_additional_exception_information(exception),
            **_get_command_metrics(),
        }
    )


@ignore_exceptions()
def flush_telemetry():
    _telemetry.flush()
