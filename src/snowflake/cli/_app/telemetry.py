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
from typing import Any, Dict, List, Optional, Tuple, Union

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
from snowflake.cli.api.utils.tty import is_tty_interactive
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
    COMMAND_CI_INTEGRATION_VERSION = "command_ci_integration_version"
    COMMAND_CI_AUTH_TYPE = "command_ci_auth_type"
    # Auth type resolved for the command's connection, recorded for every
    # command that opens one (not just the CI/CD integrations); see
    # _get_auth_type.
    COMMAND_AUTH_TYPE = "command_auth_type"
    COMMAND_AGENT_ENVIRONMENT = "command_agent_environment"
    COMMAND_AGENT_SESSION_ID = "command_agent_session_id"
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
    # Which "snow app" product flow ran for this command:
    # "native_app" for Native App (application / application package entities),
    # "snowflake_app" for Snowflake App Runtime (snowflake-app entities).
    # Only emitted for "snow app *" commands; absent for everything else.
    #
    # Note: the field appears on TelemetryEvent.CMD_EXECUTION_RESULT and
    # CMD_EXECUTION_ERROR but NOT on CMD_EXECUTION, because the routing
    # decorators that resolve the flow run inside the command callable --
    # i.e. AFTER log_command_usage fires in pre_execute. Consumers who need
    # the flow on the executing_command event should join on
    # COMMAND_EXECUTION_ID across the three events for an invocation.
    APP_FLOW = "app_flow"


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

    info: TelemetryDict = {
        CLITelemetryField.COMMAND: command_path,
        CLITelemetryField.COMMAND_GROUP: command_path[0],
        CLITelemetryField.COMMAND_FLAGS: command_flags,
        CLITelemetryField.COMMAND_OUTPUT_TYPE: format_value.value,
        CLITelemetryField.PROJECT_DEFINITION_VERSION: str(_get_definition_version()),
        CLITelemetryField.MODE: _get_cli_running_mode(),
    }

    app_flow = _get_app_flow()
    if app_flow is not None:
        info[CLITelemetryField.APP_FLOW] = app_flow

    return info


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


def _get_app_flow() -> str | None:
    """Return the resolved "snow app" product flow for the current command.

    See ``snowflake.cli._plugins.nativeapp.v2_conversions.compat.AppFlow``;
    this returns ``"native_app"``, ``"snowflake_app"``, or ``None`` for any
    command outside the ``snow app`` group (or when the routing helpers
    haven't run yet).
    """
    try:
        return get_cli_context().app_flow
    except AttributeError:
        # CLI context not yet populated (e.g. during early startup or in
        # contrived test scenarios where the manager was reset). Treat as
        # "no flow recorded" rather than failing the whole telemetry call.
        return None


def _is_env_truthy(name: str) -> bool:
    """Check if an environment variable has a truthy value."""
    return os.environ.get(name, "").lower() in ("yes", "true", "1", "on")


def _get_ci_environment_type() -> str:
    """Detect CI/CD environment type based on environment variables."""
    if "SF_GITHUB_ACTION" in os.environ:
        return "SF_GITHUB_ACTION"
    if "SF_GITLAB_COMPONENT" in os.environ:
        return "SF_GITLAB_COMPONENT"
    if "SF_ADO_EXTENSION" in os.environ:
        return "SF_ADO_EXTENSION"
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
    if is_tty_interactive():
        return "LOCAL"
    return "UNKNOWN"


def _get_ci_integration_version() -> str:
    """Get the version of the official Snowflake CI/CD integration, if any."""
    return os.environ.get("SF_CICD_INTEGRATION_VERSION", "")


# Canonical auth-type tokens emitted by the official Snowflake CI/CD integrations
# via SF_CICD_AUTH_TYPE: "oidc", "key_pair", "password", "externalbrowser",
# "oauth", "programmatic_access_token". The set is intentionally open -- unknown
# values are still recorded rather than dropped -- but integrations should reuse
# these so the field stays consistent across GitHub Actions, Azure DevOps and the
# GitLab component.
def _get_ci_auth_type() -> str:
    """Get the auth type the official Snowflake CI/CD integration configured, if any.

    Integrations set ``SF_CICD_AUTH_TYPE`` on their OIDC/workload-identity path
    (mirroring how they already set ``SNOWFLAKE_AUTHENTICATOR``); credential
    fallbacks the integration never configures leave it unset.

    This helper is the single resolution point for the value: a future change can
    fall back to inferring the actual authenticator from the resolved connection
    when the env var is absent, without touching the integrations or the field.
    """
    return os.environ.get("SF_CICD_AUTH_TYPE", "").strip().lower()


# Maps the connector's resolved authenticator (SnowflakeConnection._authenticator,
# a normalized upper-case token) to a stable, low-cardinality telemetry value. The
# vocabulary is deliberately aligned with the command_ci_auth_type tokens (see
# _get_ci_auth_type) so the CI-declared and the actually-resolved auth types can be
# analyzed together. The set is open: an unrecognized non-empty token is passed
# through lower-cased rather than dropped, mirroring how the CI field treats
# unknown values.
_AUTH_TYPE_BY_AUTHENTICATOR = {
    "SNOWFLAKE": "password",
    "SNOWFLAKE_JWT": "key_pair",
    "EXTERNALBROWSER": "externalbrowser",
    "OAUTH": "oauth",
    "OAUTH_AUTHORIZATION_CODE": "oauth",
    "OAUTH_CLIENT_CREDENTIALS": "oauth",
    "USERNAME_PASSWORD_MFA": "username_password_mfa",
    "PROGRAMMATIC_ACCESS_TOKEN": "programmatic_access_token",
    "PAT_WITH_EXTERNAL_SESSION": "programmatic_access_token",
    "WORKLOAD_IDENTITY": "workload_identity",
}


def _normalize_auth_type(authenticator: str) -> str:
    """Normalize a connector authenticator token to a telemetry-safe auth type.

    ``authenticator`` is the value the connector resolved
    (``SnowflakeConnection._authenticator``). Known tokens map to the shared
    vocabulary; the Okta authenticator -- whose token is the customer's Okta
    *URL* -- is collapsed to ``"okta"`` so the URL never reaches telemetry.
    Unrecognized non-empty tokens are passed through lower-cased; a blank value
    yields ``""``.
    """
    token = authenticator.strip()
    if not token:
        return ""
    mapped = _AUTH_TYPE_BY_AUTHENTICATOR.get(token.upper())
    if mapped:
        return mapped
    # The only URL-valued authenticator is Okta (e.g. https://<org>.okta.com);
    # collapse anything URL-like so a customer endpoint never reaches telemetry.
    if "://" in token:
        return "okta"
    return token.lower()


def _get_auth_type() -> str:
    """Get the authentication type actually used for the current command.

    Reads the authenticator the connector resolved on the live connection --
    after config, CLI flags and ``SNOWFLAKE_*`` env vars are merged and the
    default applied -- so it reflects what was really used rather than only what
    was explicitly configured (``connection_context.authenticator`` is unset for
    the default password case and for env-var-driven auth). Unlike
    ``command_ci_auth_type`` this is not limited to the official CI/CD
    integrations: it is recorded for every command that opens a connection,
    which is what telemetry needs to segment auth usage (password vs key_pair vs
    oauth vs externalbrowser vs ...).

    Reads the connection only if one is *already* open: telemetry must never be
    the reason the CLI authenticates. ``get_cli_context().connection`` is lazy and
    would dial on a miss, which for externalbrowser/OAuth means launching a
    browser -- and the payload is also built for the pre-command event, before the
    command body has connected. So the field is empty on the pre-command event and
    populated on the post-command one, where the connection genuinely exists. Any
    failure, no open connection, or a connection that does not expose a string
    authenticator yields ``""``.
    """
    try:
        connection = get_cli_context().connection_if_open
    except Exception:
        return ""
    authenticator = getattr(connection, "_authenticator", None)
    if not isinstance(authenticator, str):
        return ""
    return _normalize_auth_type(authenticator)


# Authenticators whose login flow needs a human present: opening a connection
# for these launches a browser or pushes an MFA prompt. Telemetry is never worth
# that, so when one of them is configured we only piggyback on a connection the
# command itself opened (see CLITelemetryClient._telemetry). Everything else --
# password, key pair, PAT, workload identity, token-based OAuth, native Okta --
# completes without user interaction and keeps today's behaviour.
_INTERACTIVE_AUTHENTICATORS = frozenset(
    {
        "EXTERNALBROWSER",
        "OAUTH_AUTHORIZATION_CODE",
        "USERNAME_PASSWORD_MFA",
    }
)


def _configured_authenticator() -> str:
    """Resolve the configured authenticator without opening a connection.

    Resolution order mirrors :func:`connect_to_snowflake`, so the answer matches
    the authenticator a dial would really use: flags first, then the named
    connection's config, and ``SNOWFLAKE_AUTHENTICATOR`` only as a fallback when
    neither set one. ``authenticator`` is in ``SUPPORTED_ENV_OVERRIDES``, which
    step (2) of that function applies only to keys still missing -- so reading the
    env var first would let a leftover ``SNOWFLAKE_AUTHENTICATOR=snowflake``
    mask an ``authenticator = "externalbrowser"`` in ``config.toml`` and put the
    browser back. It still needs an explicit read because the CLI's config layer
    merges only ``SNOWFLAKE_CONNECTIONS_*`` into the connection context.

    Reads config on a *clone* of the connection context: ``update_from_config``
    fills in missing fields, and doing that to the shared context would change
    the connection cache key the command body later computes.
    """
    context = get_cli_context().connection_context.clone()
    context.validate_and_complete()
    context.update_from_config()
    from_context = (context.authenticator or "").strip()
    if from_context:
        return from_context.upper()
    return os.environ.get("SNOWFLAKE_AUTHENTICATOR", "").strip().upper()


def _telemetry_may_open_connection() -> bool:
    """Whether telemetry may open a connection when none is open yet.

    Telemetry rides on the connector's per-connection telemetry channel, so
    emitting an event requires a connection. That is acceptable when logging in
    is silent, but not when it would prompt the user -- so this gates the dial
    rather than the send. If the authenticator cannot be determined, assume the
    worst and stay quiet: a lost telemetry event is cheaper than an unexpected
    browser window.
    """
    try:
        return _configured_authenticator() not in _INTERACTIVE_AUTHENTICATORS
    except Exception:
        return False


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


def _get_agent_session_id() -> str:
    """Return the detected agent's session ID, or empty string."""
    agent = _detect_agent_environment()
    if agent == "CORTEX":
        return os.environ.get("CORTEX_SESSION_ID", "").strip()
    if agent == "CLAUDE_CODE":
        return os.environ.get("CLAUDE_CODE_SESSION_ID", "").strip()
    return ""


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
    # Cap on events recorded before a connection existed, waiting to be handed
    # to the connector's telemetry channel (see _drain). A runaway guard rather
    # than a batch size: a single run emits at most three events (usage, error,
    # result), and the cap stays well under the connector's own
    # DEFAULT_FORCE_FLUSH_SIZE of 100, so a drain cannot force a larger upload
    # than the channel already sends on its own.
    _PENDING_LIMIT = 32

    def __init__(self):
        self._pending: List[Tuple[Dict[str, Any], int]] = []

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
            CLITelemetryField.COMMAND_CI_INTEGRATION_VERSION: _get_ci_integration_version(),
            CLITelemetryField.COMMAND_CI_AUTH_TYPE: _get_ci_auth_type(),
            CLITelemetryField.COMMAND_AUTH_TYPE: _get_auth_type(),
            CLITelemetryField.COMMAND_AGENT_ENVIRONMENT: _detect_agent_environment(),
            CLITelemetryField.COMMAND_AGENT_SESSION_ID: _get_agent_session_id(),
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
        """The connector's telemetry channel, or None if nothing is connected yet.

        The channel lives on a SnowflakeConnection, and reaching for it through
        the lazy `connection` accessor made *telemetry* dial. With an
        externalbrowser/OAuth default that launched a browser on every command
        running through SnowTyper -- including ones needing no connection, e.g.
        `snow connection list` -- and broke headless/CI runs, with the resulting
        error swallowed by ignore_exceptions so the command still looked fine.

        So prefer a connection that is already open, and only fall back to
        opening one when logging in is silent (_telemetry_may_open_connection).
        """
        connection = self._ctx.connection_if_open
        if connection is None and _telemetry_may_open_connection():
            connection = self._ctx.connection
        if connection is None:
            return None
        return connection._telemetry  # noqa

    def send(self, payload: TelemetryDict):
        # Timestamp now, but keep the raw dict: _drain backfills the fields that
        # are only knowable once a connection exists.
        self._pending.append(
            (self.generate_telemetry_data_dict(payload), get_time_millis())
        )
        # Keep the newest events if a run somehow never connects; the list holds
        # two entries for a command that succeeds (usage + result) and three when
        # it also reports an error.
        del self._pending[: -self._PENDING_LIMIT]
        self._drain()

    def _drain(self):
        """Hand any buffered events to the channel, if there is one.

        Buffering is what keeps this fix from costing coverage: the pre-command
        event is generated before the command body connects, so dropping events
        that find no open channel would lose a usage event for *every* command.
        Instead they are held and drained once the command opens its connection
        (log_command_result and flush_telemetry both drain). Commands that never
        connect emit nothing -- which is the intended behaviour change: telemetry
        is no longer worth an unrequested login.
        """
        telemetry = self._telemetry
        if telemetry is None:
            return
        pending, self._pending = self._pending, []
        auth_type_field = CLITelemetryField.COMMAND_AUTH_TYPE.value
        for message, timestamp in pending:
            # An event buffered before the connection opened could not resolve the
            # authenticator yet; fill it in now that one is available, so the field
            # is not lost on the pre-command event.
            if not message.get(auth_type_field):
                message[auth_type_field] = _get_auth_type()
            telemetry.try_add_log_to_batch(
                TelemetryData.from_telemetry_data_dict(
                    from_dict=message, timestamp=timestamp
                )
            )

    def flush(self):
        try:
            self._drain()
            telemetry = self._telemetry
            if telemetry is not None:
                telemetry.send_batch()
        finally:
            # The command is over, so anything still buffered belongs to a run
            # that never opened a connection and can never be sent. Drop it:
            # this client is a module-level singleton, and leaving events queued
            # would attribute them to whatever command runs next in the same
            # process.
            self._pending.clear()


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
