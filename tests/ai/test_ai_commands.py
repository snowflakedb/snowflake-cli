# Copyright (c) 2025 Snowflake Inc.
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
import logging
import re
import shlex
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli._plugins.ai.commands import (
    AGENT_ARGS_FLAG,
    AGENT_NAME_HEADER,
    APPLICATION_HEADER,
    APPLICATION_NAME,
    CLAUDE_AGENT_NAME,
    CLAUDE_CONFLICTING_ENV,
    CLAUDE_TELEMETRY_ENV,
    GATEWAY_PATH_TEMPLATE,
    OPENCODE_AGENT_NAME,
    OPENCODE_CONFLICTING_ENV,
    OPENCODE_DEFAULT_MODEL,
    PAT_AUTHENTICATOR,
)
from snowflake.cli.api.secret import SecretType
from snowflake.connector.auth.oauth_code import AuthByOauthCode
from snowflake.connector.token_cache import TokenKey, TokenType

CLAUDE_PATH = "/usr/local/bin/claude"
FAKE_TOKEN = "fake-session-token"
FAKE_HOST = "myacct.us-west-2.aws.snowflakecomputing.com"
EXPECTED_BASE_URL = (
    f"https://{FAKE_HOST}{GATEWAY_PATH_TEMPLATE.format(name='SNOWFLAKE')}"
)
# OpenCode's SDK appends only the operation path, so the derived base URL carries the
# API version; `snow ai claude` passes the bare gateway path instead.
EXPECTED_OPENCODE_BASE_URL = f"{EXPECTED_BASE_URL.rstrip('/')}/v1"

_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    """Strips ANSI colour so assertions do not depend on terminal rendering.

    Rich colourises option names, which can split a flag across escape sequences and
    break a plain substring check even though the text is present.
    """
    return _ANSI.sub("", text)


@contextmanager
def _patch_agent_run(returncode: int = 0):
    """Patches only the subprocess this plugin uses to launch the agent.

    Patching "...commands.subprocess.run" would reach too far: commands.subprocess is
    the stdlib module object, so setting its run() is process-wide, and
    subprocess.check_output() is built on run(). The CLI shells out to icacls to check
    config-file permissions on Windows, so a process-wide mock made check_output()
    return the mock's stdout - None - and the permission check raised TypeError in
    re.finditer before the command under test ran.

    Replacing the `subprocess` name in this plugin's namespace instead leaves every
    other caller on the real module, because they bind it through their own import.
    """
    with mock.patch("snowflake.cli._plugins.ai.commands.subprocess") as module:
        module.run.return_value = subprocess.CompletedProcess(
            args=[], returncode=returncode
        )
        yield module.run


@pytest.fixture
def mock_which():
    with mock.patch(
        "snowflake.cli._plugins.ai.commands.shutil.which", return_value=CLAUDE_PATH
    ) as patched:
        yield patched


@pytest.fixture
def mock_run():
    with _patch_agent_run() as patched:
        yield patched


@pytest.fixture(autouse=True)
def mock_connection():
    """Stands in for a live connection, so no real Snowflake login is attempted."""
    conn = mock.Mock()
    conn.host = FAKE_HOST
    with mock.patch(
        "snowflake.cli._plugins.ai.commands._resolve_connection",
        return_value=(conn, None),
    ) as patched:
        yield patched


@pytest.fixture
def mock_token():
    """Stands in for a connection that yields a usable bearer credential.

    Patches the resolved credential rather than a connection type, so tests about
    environment and config are not coupled to which authenticator produced it. The
    credential logic itself is covered separately below.
    """
    with mock.patch(
        "snowflake.cli._plugins.ai.commands._resolve_bearer_token",
        return_value=SecretType(FAKE_TOKEN),
    ) as patched:
        yield patched


@pytest.fixture(autouse=True)
def no_notice_sleep():
    """Keeps the OpenCode pre-launch notice from adding seconds to every test."""
    with mock.patch("snowflake.cli._plugins.ai.commands.time.sleep") as patched:
        yield patched


@pytest.fixture
def mock_connection_context():
    """Controls the resolved connection config that drives credential selection.

    Patches the resolved view rather than get_cli_context, because a named
    connection's authenticator only appears after the config is merged in.
    """

    def _apply(authenticator=None, token=None, token_file_path=None):
        context = mock.Mock()
        context.authenticator = authenticator
        context.token = token
        context.token_file_path = token_file_path
        return mock.patch(
            "snowflake.cli._plugins.ai.commands._effective_connection_context",
            return_value=context,
        )

    return _apply


def _env_of(mock_run):
    return mock_run.call_args.kwargs["env"]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX OAuth helper")
@pytest.mark.parametrize("agent", ["claude", "opencode"])
def test_renewal_accepts_cli_diagnostic_defaults(
    runner, mock_which, mock_run, mock_token, mock_connection, monkeypatch, agent
):
    connection = mock_connection.return_value[0]
    connection.account = "test"
    connection.user = "TEST"
    connection.role = "ANALYST"
    setattr(connection.auth_class, "_idp_host", FAKE_HOST)
    configuration = {
        "account": "test",
        "host": FAKE_HOST,
        "user": "TEST",
        "role": "ANALYST",
        "authenticator": "oauth_authorization_code",
        "client_store_temporary_credential": True,
        "oauth_enable_refresh_tokens": True,
    }
    with mock.patch(
        "snowflake.cli._app.snow_connector.get_connection_dict",
        return_value=configuration,
    ):
        result = runner.invoke(["ai", agent, "-c", "test"])

    assert result.exit_code == 0, result.output
    mock_run.assert_called_once()
    binding = json.loads(_env_of(mock_run)["SNOWFLAKE_AI_OAUTH_BINDING"])
    assert set(binding) == {"account", "host", "user", "role"}


def test_launches_claude_with_all_required_env_vars(
    runner, mock_which, mock_run, mock_token
):
    result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    env = _env_of(mock_run)
    for key, value in CLAUDE_TELEMETRY_ENV.items():
        assert env[key] == value
    assert env["ANTHROPIC_BASE_URL"] == EXPECTED_BASE_URL
    assert env["ANTHROPIC_AUTH_TOKEN"] == FAKE_TOKEN
    assert env["OTEL_TRACES_EXPORTER"] == "console"


def _custom_headers(mock_run) -> dict:
    """Parses ANTHROPIC_CUSTOM_HEADERS into a mapping."""
    raw = _env_of(mock_run)["ANTHROPIC_CUSTOM_HEADERS"]
    parsed = {}
    for line in raw.splitlines():
        name, _, value = line.partition(":")
        parsed[name.strip()] = value.strip()
    return parsed


def test_sends_the_attribution_headers(runner, mock_which, mock_run, mock_token):
    """Identifies the launcher and the agent it started, for gateway-side visibility."""
    result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    headers = _custom_headers(mock_run)
    assert headers[APPLICATION_HEADER] == APPLICATION_NAME
    assert headers[AGENT_NAME_HEADER] == CLAUDE_AGENT_NAME


def test_existing_custom_headers_are_preserved(
    runner, mock_which, mock_run, mock_token, monkeypatch
):
    """A parent process (sf-cli writes these too) keeps its values: first write wins."""
    monkeypatch.setenv(
        "ANTHROPIC_CUSTOM_HEADERS",
        f"{APPLICATION_HEADER}: someone-else\nX-Their-Header: keep-me",
    )

    result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    headers = _custom_headers(mock_run)
    assert headers[APPLICATION_HEADER] == "someone-else"
    assert headers["X-Their-Header"] == "keep-me"
    # Ours are still added where they do not collide.
    assert headers[AGENT_NAME_HEADER] == CLAUDE_AGENT_NAME


def test_forwards_agent_args_to_claude(runner, mock_which, mock_run, mock_token):
    result = runner.invoke(["ai", "claude", "--agent-args", "--resume", "-p", "hello"])

    assert result.exit_code == 0, result.output
    assert mock_run.call_args.args[0] == [CLAUDE_PATH, "--resume", "-p", "hello"]


def test_agent_args_forwards_flags_snow_also_defines(
    runner, mock_which, mock_run, mock_token
):
    """--debug and --verbose are snow globals; after the terminator they are claude's."""
    result = runner.invoke(["ai", "claude", "--agent-args", "--debug", "--verbose"])

    assert result.exit_code == 0, result.output
    assert mock_run.call_args.args[0] == [CLAUDE_PATH, "--debug", "--verbose"]


def test_agent_args_as_first_argument_launches_rather_than_printing_help(
    runner, mock_which, mock_run, mock_token
):
    """An all-agent invocation leaves click an empty head.

    click prints help and exits when it is handed no arguments and no_args_is_help
    is set, which would swallow the tail. The commands pin it off for this reason.
    """
    result = runner.invoke(["ai", "claude", "--agent-args", "-p", "hi"])

    assert result.exit_code == 0, result.output
    assert mock_run.call_args.args[0] == [CLAUDE_PATH, "-p", "hi"]


def test_agent_args_with_empty_tail_forwards_nothing(
    runner, mock_which, mock_run, mock_token
):
    result = runner.invoke(["ai", "claude", "--agent-args"])

    assert result.exit_code == 0, result.output
    assert mock_run.call_args.args[0] == [CLAUDE_PATH]


def test_only_the_first_agent_args_delimits(runner, mock_which, mock_run, mock_token):
    """A second occurrence is just another token for the agent."""
    result = runner.invoke(["ai", "claude", "--agent-args", "--agent-args", "literal"])

    assert result.exit_code == 0, result.output
    assert mock_run.call_args.args[0] == [CLAUDE_PATH, "--agent-args", "literal"]


def test_snow_options_after_the_terminator_are_forwarded_not_honoured(
    runner, mock_which, mock_run, mock_token
):
    """--role past the terminator reaches claude and is never parsed by snow."""
    result = runner.invoke(
        ["ai", "claude", "--agent-args", "--role", "claimed-by-agent"]
    )

    assert result.exit_code == 0, result.output
    assert mock_run.call_args.args[0] == [
        CLAUDE_PATH,
        "--role",
        "claimed-by-agent",
    ]


def test_snow_options_before_the_terminator_are_still_parsed(
    runner, mock_which, mock_run, mock_token
):
    """A snow global ahead of the terminator is snow's and is not forwarded."""
    result = runner.invoke(
        ["ai", "claude", "--role", "my-role", "--agent-args", "--resume"]
    )

    assert result.exit_code == 0, result.output
    assert mock_run.call_args.args[0] == [CLAUDE_PATH, "--resume"]


def test_snow_global_before_the_terminator_belongs_to_snow(
    runner, mock_which, mock_run, mock_token
):
    """Documents the boundary: --verbose ahead of the terminator is snow's own flag.

    It is consumed by snow rather than forwarded, which is why the terminator is the
    only supported way to reach the agent.
    """
    result = runner.invoke(["ai", "claude", "--verbose"])

    assert result.exit_code == 0, result.output
    assert mock_run.call_args.args[0] == [CLAUDE_PATH]


def test_unknown_flag_without_the_terminator_errors_with_a_hint(
    runner, mock_which, mock_run, mock_token
):
    result = runner.invoke(["ai", "claude", "--resume"])

    assert result.exit_code != 0
    assert AGENT_ARGS_FLAG in _plain(result.output)
    mock_run.assert_not_called()


def test_double_dash_is_no_longer_supported_and_points_at_the_terminator(
    runner, mock_which, mock_run, mock_token
):
    """`--` used to work, so the failure has to name the replacement."""
    result = runner.invoke(["ai", "claude", "--", "--debug"])

    assert result.exit_code != 0
    assert AGENT_ARGS_FLAG in _plain(result.output)
    mock_run.assert_not_called()


def test_unsupported_authenticator_is_fatal(
    runner, mock_which, mock_run, mock_connection_context
):
    """Refuse rather than pass a credential the REST layer answers with a bare 401."""
    with mock_connection_context(authenticator="externalbrowser"):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code != 0
    assert PAT_AUTHENTICATOR in result.output
    mock_run.assert_not_called()


def test_oauth_connection_requires_matching_connector_authenticator(
    runner, mock_which, mock_run, mock_connection_context
):
    """Do not search unrelated caches when the login authenticator is unexpected."""
    with mock_connection_context(authenticator="oauth_authorization_code"):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code != 0
    assert "expected OAuth" in result.output
    mock_run.assert_not_called()


@pytest.mark.parametrize("exporter", ["otlp", "console", "none", "otlp,console", ""])
def test_existing_traces_exporter_is_not_overridden(
    runner, mock_which, mock_run, mock_token, exporter
):
    with mock.patch.dict("os.environ", {"OTEL_TRACES_EXPORTER": exporter}):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    assert _env_of(mock_run)["OTEL_TRACES_EXPORTER"] == exporter


def test_traces_exporter_defaults_to_console_when_unset(
    runner, mock_which, mock_run, mock_token, monkeypatch
):
    monkeypatch.delenv("OTEL_TRACES_EXPORTER", raising=False)
    result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    env = _env_of(mock_run)
    assert env["OTEL_TRACES_EXPORTER"] == "console"
    assert env["CLAUDE_CODE_PROPAGATE_TRACEPARENT"] == "1"


@pytest.mark.parametrize("agent", ["claude", "opencode"])
def test_exporter_configuration_is_preserved(
    runner, mock_which, mock_run, mock_token, monkeypatch, agent
):
    settings = {
        "OTEL_TRACES_EXPORTER": "otlp",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://collector.example/v1/traces",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "http/json",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS": "Authorization=Bearer collector-token",
        "OPENCODE_OTLP_ENDPOINT": "https://collector.example",
        "OPENCODE_OTLP_PROTOCOL": "http/protobuf",
        "OPENCODE_OTLP_HEADERS": "Authorization=Bearer collector-token",
        "OPENCODE_DISABLE_TRACES": "tool",
    }
    for name, value in settings.items():
        monkeypatch.setenv(name, value)

    result = runner.invoke(["ai", agent])

    assert result.exit_code == 0, result.output
    env = _env_of(mock_run)
    for name, value in settings.items():
        assert env[name] == value


@pytest.mark.parametrize("agent", ["claude", "opencode"])
def test_no_export_destination_or_credentials_are_injected(
    runner, mock_which, mock_run, mock_token, monkeypatch, agent
):
    settings = (
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        "OPENCODE_OTLP_ENDPOINT",
        "OPENCODE_OTLP_HEADERS",
    )
    for name in settings:
        monkeypatch.delenv(name, raising=False)

    result = runner.invoke(["ai", agent])

    assert result.exit_code == 0, result.output
    env = _env_of(mock_run)
    for name in settings:
        assert name not in env


def test_propagates_claude_exit_code(runner, mock_which, mock_token):
    with _patch_agent_run(returncode=42):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 42


def test_errors_when_claude_not_installed(runner, mock_run, mock_token):
    with mock.patch(
        "snowflake.cli._plugins.ai.commands.shutil.which", return_value=None
    ):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code != 0
    assert "claude command not found" in result.output
    mock_run.assert_not_called()


def test_inherits_caller_environment(runner, mock_which, mock_run, mock_token):
    with mock.patch.dict("os.environ", {"SOME_USER_VAR": "kept"}):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    assert _env_of(mock_run)["SOME_USER_VAR"] == "kept"


def test_base_url_is_derived_from_connection_host(
    runner, mock_which, mock_run, mock_token
):
    result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    assert _env_of(mock_run)["ANTHROPIC_BASE_URL"] == EXPECTED_BASE_URL
    assert FAKE_HOST in _env_of(mock_run)["ANTHROPIC_BASE_URL"]


def test_connection_failure_is_fatal(runner, mock_which, mock_run, mock_connection):
    mock_connection.return_value = (None, "boom")
    result = runner.invoke(["ai", "claude"])

    assert result.exit_code != 0
    assert "Could not connect" in result.output
    mock_run.assert_not_called()


# --- snow ai opencode ---------------------------------------------------------


def _opencode_config(mock_run):
    return json.loads(_env_of(mock_run)["OPENCODE_CONFIG_CONTENT"])


def test_opencode_injects_provider_config(runner, mock_which, mock_run, mock_token):
    result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 0, result.output
    cfg = _opencode_config(mock_run)
    options = cfg["provider"]["snowflake-cortex"]["options"]
    assert options["baseURL"] == EXPECTED_OPENCODE_BASE_URL
    assert options["apiKey"] == FAKE_TOKEN


def test_opencode_injects_only_gateway_defaults_without_external_plugins(
    runner, mock_which, mock_run, mock_token
):
    result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 0, result.output
    cfg = _opencode_config(mock_run)
    assert set(cfg.keys()) == {"$schema", "model", "small_model", "provider"}


def test_opencode_does_not_inject_plugin_when_connection_telemetry_is_disabled(
    runner, mock_which, mock_run, mock_token, mock_connection
):
    _connection_with(mock_connection, telemetry_enabled=False)

    result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 0, result.output
    cfg = _opencode_config(mock_run)
    assert "plugin" not in cfg
    assert cfg["provider"]["snowflake-cortex"]["options"]["apiKey"] == FAKE_TOKEN


def test_opencode_does_not_set_claude_env_vars(
    runner, mock_which, mock_run, mock_token
):
    result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 0, result.output
    env = _env_of(mock_run)
    assert "ANTHROPIC_AUTH_TOKEN" not in env
    assert "ANTHROPIC_BASE_URL" not in env
    for key in CLAUDE_TELEMETRY_ENV:
        assert key not in env


def test_opencode_forwards_agent_args(runner, mock_which, mock_run, mock_token):
    result = runner.invoke(["ai", "opencode", "--agent-args", "debug", "config"])

    assert result.exit_code == 0, result.output
    assert mock_run.call_args.args[0] == [CLAUDE_PATH, "debug", "config"]


def test_opencode_agent_args_forwards_colliding_flags(
    runner, mock_which, mock_run, mock_token
):
    """--port is a snow global and would otherwise be swallowed with its value."""
    result = runner.invoke(
        ["ai", "opencode", "--agent-args", "serve", "--port", "3000"]
    )

    assert result.exit_code == 0, result.output
    assert mock_run.call_args.args[0] == [CLAUDE_PATH, "serve", "--port", "3000"]


def test_opencode_model_override_is_left_to_the_native_flag(
    runner, mock_which, mock_run, mock_token
):
    """snow defines no --model; -m is OpenCode's own and is forwarded untouched.

    small_model stays pinned so overriding the main model cannot break the title
    request against a model the gateway does not serve.
    """
    result = runner.invoke(
        ["ai", "opencode", "--agent-args", "-m", "snowflake-cortex/passed-through"]
    )

    assert result.exit_code == 0, result.output
    cfg = _opencode_config(mock_run)
    assert cfg["small_model"] == f"snowflake-cortex/{OPENCODE_DEFAULT_MODEL}"
    assert mock_run.call_args.args[0] == [
        CLAUDE_PATH,
        "-m",
        "snowflake-cortex/passed-through",
    ]


def test_opencode_propagates_exit_code(runner, mock_which, mock_token):
    with _patch_agent_run(returncode=17):
        result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 17


def test_opencode_errors_when_binary_missing(runner, mock_run, mock_token):
    with (
        mock.patch(
            "snowflake.cli._plugins.ai.commands.shutil.which", return_value=None
        ),
        mock.patch(
            "snowflake.cli._plugins.ai.commands.OPENCODE_FALLBACK_BIN",
            Path("/nonexistent/opencode"),
        ),
    ):
        result = runner.invoke(["ai", "opencode"])

    assert result.exit_code != 0
    assert "opencode command not found" in result.output
    mock_run.assert_not_called()


def test_opencode_pins_model_and_small_model(runner, mock_which, mock_run, mock_token):
    """The title request must not fall back to a model the gateway may not serve."""
    result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 0, result.output
    cfg = _opencode_config(mock_run)
    assert cfg["model"] == f"snowflake-cortex/{OPENCODE_DEFAULT_MODEL}"
    assert cfg["small_model"] == cfg["model"]
    assert OPENCODE_DEFAULT_MODEL == "openai-gpt-5.4"


def test_opencode_sets_account_so_compat_shims_attach(
    runner, mock_which, mock_run, mock_token
):
    """OpenCode's loader skips its Snowflake compat fetch wrapper without an account.

    That wrapper is what rewrites max_tokens to max_completion_tokens, so dropping it
    puts a body on the wire that the Cortex-compatible endpoint does not accept.
    """
    result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 0, result.output
    options = _opencode_config(mock_run)["provider"]["snowflake-cortex"]["options"]
    assert options["account"] == FAKE_HOST


def test_opencode_appends_version_to_derived_base_url(
    runner, mock_which, mock_run, mock_token
):
    """The AI SDK appends only /chat/completions, so /v1 must be in the base URL."""
    result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 0, result.output
    options = _opencode_config(mock_run)["provider"]["snowflake-cortex"]["options"]
    assert options["baseURL"] == f"{EXPECTED_BASE_URL.rstrip('/')}/v1"


def test_opencode_default_model_disables_reasoning_for_tool_compatibility(
    runner, mock_which, mock_run, mock_token
):
    result = runner.invoke(["ai", "opencode"])
    assert result.exit_code == 0, result.output
    provider = _opencode_config(mock_run)["provider"]["snowflake-cortex"]
    assert provider["models"] == {
        OPENCODE_DEFAULT_MODEL: {"options": {"reasoningEffort": "none"}}
    }
    assert "reasoningEffort" not in provider["options"]


def test_claude_base_url_has_no_version_suffix(
    runner, mock_which, mock_run, mock_token
):
    """Claude Code's SDK appends /v1/messages itself, so the base must omit /v1."""
    result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    assert _env_of(mock_run)["ANTHROPIC_BASE_URL"] == EXPECTED_BASE_URL
    assert not _env_of(mock_run)["ANTHROPIC_BASE_URL"].rstrip("/").endswith("/v1")


def test_opencode_always_sends_the_agent_name_header(
    runner, mock_which, mock_run, mock_token
):
    """OpenCode takes no auth env vars, so the headers ride in the injected config."""
    result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 0, result.output
    options = _opencode_config(mock_run)["provider"]["snowflake-cortex"]["options"]
    assert options["headers"] == {
        APPLICATION_HEADER: APPLICATION_NAME,
        AGENT_NAME_HEADER: OPENCODE_AGENT_NAME,
    }


def test_opencode_agent_name_differs_from_claude(
    runner, mock_which, mock_run, mock_token
):
    """The header only distinguishes agents if the two values differ."""
    assert CLAUDE_AGENT_NAME != OPENCODE_AGENT_NAME


def test_pat_connection_uses_the_configured_token(
    runner, mock_which, mock_run, mock_connection_context
):
    """A PAT is a documented connection parameter, so no connector internals needed."""
    with mock_connection_context(authenticator=PAT_AUTHENTICATOR, token="pat-abc"):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    assert _env_of(mock_run)["ANTHROPIC_AUTH_TOKEN"] == "pat-abc"


def test_pat_authenticator_is_matched_case_insensitively(
    runner, mock_which, mock_run, mock_connection_context
):
    """The CLI compares authenticators lowercased, so config casing must not matter."""
    with mock_connection_context(
        authenticator=PAT_AUTHENTICATOR.lower(), token="pat-abc"
    ):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    assert _env_of(mock_run)["ANTHROPIC_AUTH_TOKEN"] == "pat-abc"


def test_pat_is_read_from_token_file_path(
    runner, mock_which, mock_run, mock_connection_context, tmp_path
):
    token_file = tmp_path / "pat.txt"
    token_file.write_text("pat-from-file\n", encoding="utf-8")

    with mock_connection_context(
        authenticator=PAT_AUTHENTICATOR, token_file_path=str(token_file)
    ):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    # Trailing newline stripped, or the header would carry it.
    assert _env_of(mock_run)["ANTHROPIC_AUTH_TOKEN"] == "pat-from-file"


def test_token_file_path_takes_precedence_over_inline_token(
    runner, mock_which, mock_run, mock_connection_context, tmp_path
):
    token_file = tmp_path / "pat.txt"
    token_file.write_text("file-token", encoding="utf-8")

    with mock_connection_context(
        authenticator=PAT_AUTHENTICATOR,
        token="inline-token",
        token_file_path=str(token_file),
    ):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    assert _env_of(mock_run)["ANTHROPIC_AUTH_TOKEN"] == "file-token"


def test_pat_authenticator_without_a_token_is_fatal(
    runner, mock_which, mock_run, mock_connection_context
):
    with mock_connection_context(authenticator=PAT_AUTHENTICATOR):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code != 0
    assert "token" in result.output
    mock_run.assert_not_called()


def test_unreadable_connection_config_is_reported_as_such(
    runner, mock_which, mock_run, mock_connection
):
    """A config that cannot be read must not be reported as an unset authenticator.

    Both cases used to collapse into the empty string, which sent the user to set an
    authenticator that may already have been correct.
    """
    with mock.patch(
        "snowflake.cli._plugins.ai.commands._effective_connection_context",
        side_effect=RuntimeError("connections.toml is not valid TOML"),
    ):
        result = runner.invoke(["ai", "claude"])

    # The error box wraps and inserts border characters, so match a short fragment.
    output = " ".join(_plain(result.output).split())
    assert result.exit_code != 0
    assert "Could not read the connection configuration" in output
    assert "connections.toml is not valid" not in output
    assert "not set" not in output
    mock_run.assert_not_called()


def test_no_fallback_to_the_driver_session_token(
    runner, mock_which, mock_run, mock_connection_context, mock_connection
):
    """A connection that exposes a session token must still refuse to launch.

    The session token is rejected by the REST layer with an empty-bodied 401, so the
    old fallback turned a clear CLI failure into a confusing gateway one. Before this
    change the invocation below succeeded and shipped that doomed token to the agent.
    """
    connection = mock_connection.return_value[0]
    connection.rest.token = "session-token-should-not-be-used"

    with mock_connection_context(authenticator="username_password_mfa"):
        result = runner.invoke(["ai", "claude"])

    assert result.exit_code != 0
    assert "session-token-should-not-be-used" not in result.output
    mock_run.assert_not_called()


@pytest.mark.parametrize("agent", ["claude", "opencode"])
@pytest.mark.parametrize("enabled", [False, True])
def test_externalbrowser_rejected_even_with_legacy_experimental_opt_in(
    runner,
    mock_which,
    mock_run,
    mock_connection_context,
    mock_connection,
    monkeypatch,
    agent,
    enabled,
):
    monkeypatch.setenv(
        "SNOWFLAKE_AI_EXPERIMENTAL_EXTERNALBROWSER", "1" if enabled else "0"
    )
    mock_connection.return_value[0].rest.token = "synthetic-session-credential"
    with mock_connection_context(authenticator="externalbrowser"):
        result = runner.invoke(["ai", agent])
    assert "synthetic-session-credential" not in result.output
    assert result.exit_code != 0
    assert "Could not derive a credential" in result.output
    mock_run.assert_not_called()


def test_opencode_warns_about_claude_models_before_launching(
    runner, mock_which, mock_run, mock_token
):
    """The default model silently works around a 503, so say so while we own the TTY."""
    result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 0, result.output
    assert "503" in result.output
    assert OPENCODE_DEFAULT_MODEL in result.output


def test_claude_does_not_warn_about_opencode_models(
    runner, mock_which, mock_run, mock_token
):
    result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    assert "503" not in result.output


def test_notice_pause_is_skipped_when_not_a_tty(
    runner, mock_which, mock_run, mock_token, no_notice_sleep
):
    """Scripted and CI invocations must not pay the readability pause."""
    with mock.patch(
        "snowflake.cli._plugins.ai.commands._is_interactive", return_value=False
    ):
        result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 0, result.output
    no_notice_sleep.assert_not_called()


def test_notice_pauses_on_a_tty(
    runner, mock_which, mock_run, mock_token, no_notice_sleep
):
    with mock.patch(
        "snowflake.cli._plugins.ai.commands._is_interactive", return_value=True
    ):
        result = runner.invoke(["ai", "opencode"])

    assert result.exit_code == 0, result.output
    no_notice_sleep.assert_called_once()


def test_gateway_name_selects_the_gateway_for_claude(
    runner, mock_which, mock_run, mock_token
):
    result = runner.invoke(["ai", "claude", "--gateway-name", "OTHER_GW"])

    assert result.exit_code == 0, result.output
    assert (
        _env_of(mock_run)["ANTHROPIC_BASE_URL"]
        == f"https://{FAKE_HOST}/api/v2/aigateways/OTHER_GW/"
    )


def test_gateway_name_selects_the_gateway_for_opencode(
    runner, mock_which, mock_run, mock_token
):
    """The same name must work for both agents, so opencode re-adds its /v1."""
    result = runner.invoke(["ai", "opencode", "--gateway-name", "OTHER_GW"])

    assert result.exit_code == 0, result.output
    options = _opencode_config(mock_run)["provider"]["snowflake-cortex"]["options"]
    assert options["baseURL"] == f"https://{FAKE_HOST}/api/v2/aigateways/OTHER_GW/v1"


def test_gateway_host_always_comes_from_the_connection(
    runner, mock_which, mock_run, mock_token
):
    """The credential's destination is not caller-supplied.

    There is deliberately no way to aim an agent at an arbitrary host: the address is
    built from the connection we authenticated against, so a hostile name cannot move
    a live credential off a Snowflake endpoint.
    """
    result = runner.invoke(["ai", "claude", "--gateway-name", "SNOWFLAKE"])

    assert result.exit_code == 0, result.output
    assert _env_of(mock_run)["ANTHROPIC_BASE_URL"].startswith(f"https://{FAKE_HOST}/")


@pytest.mark.parametrize(
    "hostile_name",
    [
        "../../../v1",
        "SNOWFLAKE/../..",
        "evil.example.com",
        "SNOWFLAKE?x=1",
        "SNOWFLAKE#frag",
        "SNOW FLAKE",
        "",
    ],
)
def test_gateway_name_that_could_reshape_the_url_is_refused(
    runner, mock_which, mock_run, mock_token, hostile_name
):
    """A name is interpolated into the path, so it must not carry URL syntax."""
    result = runner.invoke(["ai", "claude", "--gateway-name", hostile_name])

    assert result.exit_code != 0
    mock_run.assert_not_called()


def test_gateway_name_does_not_change_the_credential_source(
    runner, mock_which, mock_run, mock_connection_context
):
    """Selecting a gateway must not weaken auth: the token still comes from config."""
    with mock_connection_context(authenticator="externalbrowser"):
        result = runner.invoke(["ai", "claude", "--gateway-name", "OTHER_GW"])

    assert result.exit_code != 0
    mock_run.assert_not_called()


def test_gateway_name_defaults_to_snowflake(runner, mock_which, mock_run, mock_token):
    result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    assert _env_of(mock_run)["ANTHROPIC_BASE_URL"] == EXPECTED_BASE_URL


def _connection_with(mock_connection, **attrs):
    """Repoints the connection fixture at a connection carrying the given attributes."""
    conn = mock.Mock()
    conn.host = FAKE_HOST
    for name, value in attrs.items():
        setattr(conn, name, value)
    mock_connection.return_value = (conn, None)
    return conn


def test_telemetry_is_not_forced_when_disabled_for_the_connection(
    runner, mock_which, mock_run, mock_token, mock_connection
):
    """snow's telemetry switch governs the agent's too.

    Turning a third-party tool's telemetry on for someone who turned snow's off is not
    this command's decision to make.
    """
    _connection_with(mock_connection, telemetry_enabled=False)

    result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    env = _env_of(mock_run)
    for key in CLAUDE_TELEMETRY_ENV:
        assert key not in env
    assert "OTEL_TRACES_EXPORTER" not in env


def test_telemetry_is_requested_when_enabled_for_the_connection(
    runner, mock_which, mock_run, mock_token, mock_connection
):
    _connection_with(mock_connection, telemetry_enabled=True)

    result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    env = _env_of(mock_run)
    for key, value in CLAUDE_TELEMETRY_ENV.items():
        assert env[key] == value


def test_deliberate_telemetry_setting_is_not_overridden(
    runner, mock_which, mock_run, mock_token, monkeypatch
):
    """A user who set the variable themselves meant it."""
    monkeypatch.setenv("CLAUDE_CODE_ENABLE_TELEMETRY", "0")

    result = runner.invoke(["ai", "claude"])

    assert result.exit_code == 0, result.output
    assert _env_of(mock_run)["CLAUDE_CODE_ENABLE_TELEMETRY"] == "0"


@pytest.mark.parametrize("variable", CLAUDE_CONFLICTING_ENV)
def test_claude_refuses_env_that_would_swap_the_credential(
    runner, mock_which, mock_run, mock_token, monkeypatch, variable
):
    """Silently using a credential other than the validated one is the worst outcome."""
    monkeypatch.setenv(variable, "1")

    result = runner.invoke(["ai", "claude"])

    assert result.exit_code != 0
    assert variable in _plain(result.output)
    mock_run.assert_not_called()


@pytest.mark.parametrize("variable", OPENCODE_CONFLICTING_ENV)
def test_opencode_refuses_env_that_takes_precedence_over_the_config(
    runner, mock_which, mock_run, mock_token, monkeypatch, variable
):
    """The provider reads these ahead of the injected config, so they must not be set."""
    monkeypatch.setenv(variable, "something")

    result = runner.invoke(["ai", "opencode"])

    assert result.exit_code != 0
    assert variable in _plain(result.output)
    mock_run.assert_not_called()


def test_opencode_config_is_wrapped_as_a_secret():
    """The JSON embeds the credential, so a stray log must render it masked."""
    from snowflake.cli._plugins.ai.commands import _build_opencode_config

    config = _build_opencode_config("https://host/gw/v1", SecretType(FAKE_TOKEN))

    assert isinstance(config, SecretType)
    assert FAKE_TOKEN not in repr(config)
    assert FAKE_TOKEN not in str(config)
    assert FAKE_TOKEN in config.value


def test_claude_env_keeps_token_masked_until_launch(caplog):
    from snowflake.cli._plugins.ai.commands import _build_env

    token = SecretType(FAKE_TOKEN)
    env = _build_env(EXPECTED_BASE_URL, token)

    assert env["ANTHROPIC_AUTH_TOKEN"] is token
    assert FAKE_TOKEN not in str(env)
    assert FAKE_TOKEN not in repr(env)
    with caplog.at_level(logging.INFO):
        logging.getLogger(__name__).info("Environment: %s", env)
    assert FAKE_TOKEN not in caplog.text
    assert "SecretType(***)" in caplog.text


@pytest.mark.parametrize("agent", ["claude", "opencode"])
def test_real_child_receives_plaintext_credential_without_printing_it(
    runner, mock_which, mock_token, agent, caplog
):
    mock_which.return_value = sys.executable
    script = """
import json
import os
import sys

agent, expected_token = sys.argv[1:]
if agent == "claude":
    actual_token = os.environ["ANTHROPIC_AUTH_TOKEN"]
else:
    config = json.loads(os.environ["OPENCODE_CONFIG_CONTENT"])
    actual_token = config["provider"]["snowflake-cortex"]["options"]["apiKey"]
    assert "plugin" not in config
sys.exit(0 if actual_token == expected_token else 7)
"""

    result = runner.invoke(
        ["ai", agent, "--agent-args", "-c", script, agent, FAKE_TOKEN]
    )

    assert result.exit_code == 0, result.output
    assert FAKE_TOKEN not in result.output
    assert FAKE_TOKEN not in caplog.text


@pytest.mark.parametrize("agent", ["claude", "opencode"])
@pytest.mark.parametrize("authenticator", ["oauth", "OAUTH"])
@pytest.mark.parametrize("from_file", [False, True])
def test_raw_oauth_static_handoff(
    runner,
    mock_which,
    mock_run,
    mock_connection_context,
    agent,
    authenticator,
    from_file,
    tmp_path,
    caplog,
):
    token_file = tmp_path / "oauth-token.txt"
    token_file.write_text("oauth-static-secret\n", encoding="utf-8")
    with mock_connection_context(
        authenticator=authenticator,
        token=None if from_file else "oauth-static-secret",
        token_file_path=str(token_file) if from_file else None,
    ):
        result = runner.invoke(["ai", agent])
    assert result.exit_code == 0, result.output
    if agent == "claude":
        token = _env_of(mock_run)["ANTHROPIC_AUTH_TOKEN"]
    else:
        token = _opencode_config(mock_run)["provider"]["snowflake-cortex"]["options"][
            "apiKey"
        ]
    assert token == "oauth-static-secret"
    assert "not refreshed" in _plain(result.output)
    assert token not in result.output
    assert token not in caplog.text


@pytest.fixture
def oauth_connection(mock_connection):
    auth = mock.Mock(spec=AuthByOauthCode)
    auth.configure_mock(
        **{
            "_idp_host": "issuer.example.test",
            "_token_cache": mock.Mock(),
            "_token_cache.retrieve.return_value": "cached-oauth-secret",
        }
    )
    connection = mock_connection.return_value[0]
    connection.auth_class = auth
    connection.user = "TEST_USER"
    connection.rest.token = "never-use-session-token"
    return connection


@pytest.mark.parametrize("agent", ["claude", "opencode"])
@pytest.mark.parametrize(
    "authenticator", ["oauth_authorization_code", "OAUTH_AUTHORIZATION_CODE"]
)
@pytest.mark.skipif(sys.platform == "win32", reason="POSIX OAuth helper")
def test_oauth_code_installs_real_renewal_wiring(
    runner,
    mock_which,
    mock_run,
    mock_connection_context,
    oauth_connection,
    agent,
    authenticator,
    caplog,
    monkeypatch,
):
    monkeypatch.delenv("CLAUDE_CODE_API_KEY_HELPER_TTL_MS", raising=False)
    oauth_connection.account = "test"
    oauth_connection.role = "ANALYST"
    oauth_connection.auth_class.configure_mock(_idp_host=FAKE_HOST)
    with mock.patch(
        "snowflake.cli._app.snow_connector.get_connection_dict",
        return_value={
            "account": "test",
            "host": FAKE_HOST,
            "user": "TEST_USER",
            "role": "ANALYST",
            "authenticator": authenticator,
            "client_store_temporary_credential": True,
            "oauth_enable_refresh_tokens": True,
        },
    ):
        result = runner.invoke(["ai", agent, "-c", "test"])
    assert result.exit_code == 0, result.output
    getattr(
        oauth_connection.auth_class, "_token_cache"
    ).retrieve.assert_called_once_with(
        TokenKey(
            user="TEST_USER",
            host=FAKE_HOST,
            tokenType=TokenType.OAUTH_ACCESS_TOKEN,
        )
    )
    env = _env_of(mock_run)
    assert json.loads(env["SNOWFLAKE_AI_OAUTH_BINDING"]) == {
        "account": "test",
        "host": FAKE_HOST,
        "user": "TEST_USER",
        "role": "ANALYST",
    }
    command = json.loads(env["SNOWFLAKE_AI_OAUTH_COMMAND"])
    assert command == [
        str(Path(sys.executable).absolute()),
        "-I",
        "-m",
        "snowflake.cli._plugins.ai.oauth",
    ]
    if agent == "claude":
        assert "ANTHROPIC_AUTH_TOKEN" not in env
        assert "CLAUDE_CODE_API_KEY_HELPER_TTL_MS" not in env
        settings = json.loads(mock_run.call_args.args[0][2])
        assert shlex.split(settings["apiKeyHelper"]) == command
        assert settings["env"]["ANTHROPIC_BASE_URL"] == EXPECTED_BASE_URL
    else:
        config = _opencode_config(mock_run)
        assert (
            config["provider"]["snowflake-cortex"]["options"]["apiKey"]
            == "snow-cli-oauth-helper-required"
        )
        assert config["plugin"][-1].endswith("opencode_oauth.mjs")
    for secret in ("cached-oauth-secret", "never-use-session-token"):
        assert secret not in result.output + caplog.text + str(env)


@pytest.mark.parametrize(
    "failure",
    [
        "cache_disabled",
        "missing",
        "empty",
        "cache_error",
        "issuer_missing",
        "user_missing",
        "wrong_auth",
    ],
)
def test_oauth_code_failures_do_not_launch_or_leak(
    runner,
    mock_which,
    mock_run,
    mock_connection_context,
    oauth_connection,
    failure,
    caplog,
):
    auth = oauth_connection.auth_class
    cache = getattr(auth, "_token_cache")
    if failure == "cache_disabled":
        auth.configure_mock(_token_cache=None)
    elif failure == "missing":
        cache.retrieve.return_value = None
    elif failure == "empty":
        cache.retrieve.return_value = " "
    elif failure == "cache_error":
        cache.retrieve.side_effect = RuntimeError("sensitive-cache-error")
    elif failure == "issuer_missing":
        auth.configure_mock(_idp_host=None)
    elif failure == "user_missing":
        oauth_connection.user = None
    else:
        oauth_connection.auth_class = object()
    with mock_connection_context(authenticator="oauth_authorization_code"):
        result = runner.invoke(["ai", "claude"])
    assert result.exit_code != 0
    assert "OAuth" in result.output
    assert "sensitive-cache-error" not in result.output + caplog.text
    assert "never-use-session-token" not in result.output + caplog.text
    mock_run.assert_not_called()


def test_missing_oauth_cache_module_preserves_pat_support(
    runner,
    mock_which,
    mock_run,
    mock_connection_context,
):
    with mock.patch.dict(sys.modules, {"snowflake.connector.token_cache": None}):
        with mock_connection_context(authenticator="oauth_authorization_code"):
            result = runner.invoke(["ai", "claude"])
        assert result.exit_code != 0
        assert "connector version" in result.output
        mock_run.assert_not_called()
        with mock_connection_context(
            authenticator=PAT_AUTHENTICATOR, token="pat-token"
        ):
            result = runner.invoke(["ai", "claude"])
        assert result.exit_code == 0, result.output
        assert _env_of(mock_run)["ANTHROPIC_AUTH_TOKEN"] == "pat-token"


@pytest.mark.parametrize(
    "authenticator",
    ["oauth", "oauth_client_credentials", "externalbrowser", "SNOWFLAKE_JWT"],
)
def test_unavailable_oauth_or_unsupported_credentials_fail(
    runner,
    mock_which,
    mock_run,
    mock_connection_context,
    authenticator,
):
    with mock_connection_context(authenticator=authenticator):
        result = runner.invoke(["ai", "claude"])
    assert result.exit_code != 0
    mock_run.assert_not_called()


@pytest.mark.parametrize("agent", ["claude", "opencode"])
@pytest.mark.parametrize("helper_ttl", [None, "60000"])
def test_renewable_oauth_launch_wiring(
    runner, mock_which, mock_run, mock_token, agent, monkeypatch, helper_ttl
):
    if helper_ttl is None:
        monkeypatch.delenv("CLAUDE_CODE_API_KEY_HELPER_TTL_MS", raising=False)
    else:
        monkeypatch.setenv("CLAUDE_CODE_API_KEY_HELPER_TTL_MS", helper_ttl)
    renewal = {
        "SNOWFLAKE_AI_OAUTH_BINDING": '{"account":"test"}',
        "SNOWFLAKE_AI_OAUTH_COMMAND": '["/trusted/python","-I","-m","snowflake.cli._plugins.ai.oauth"]',
    }
    with mock.patch(
        "snowflake.cli._plugins.ai.commands._renewal_environment", return_value=renewal
    ):
        result = runner.invoke(["ai", agent])
    assert result.exit_code == 0, result.output
    env = _env_of(mock_run)
    assert env["SNOWFLAKE_AI_OAUTH_BINDING"] == renewal["SNOWFLAKE_AI_OAUTH_BINDING"]
    if agent == "claude":
        assert "ANTHROPIC_AUTH_TOKEN" not in env
        assert env.get("CLAUDE_CODE_API_KEY_HELPER_TTL_MS") == helper_ttl
        settings = json.loads(mock_run.call_args.args[0][2])
        assert "snowflake.cli._plugins.ai.oauth" in settings["apiKeyHelper"]
        assert settings["env"] == {"ANTHROPIC_BASE_URL": EXPECTED_BASE_URL}
        assert FAKE_TOKEN not in json.dumps(settings)
    else:
        config = _opencode_config(mock_run)
        assert (
            config["provider"]["snowflake-cortex"]["options"]["apiKey"]
            == "snow-cli-oauth-helper-required"
        )
        assert config["plugin"][-1].startswith("file://")
        assert config["plugin"][-1].endswith("opencode_oauth.mjs")
    assert FAKE_TOKEN not in str(env)


def test_renewable_claude_settings_conflict_is_explicit(
    runner, mock_which, mock_run, mock_token
):
    with mock.patch(
        "snowflake.cli._plugins.ai.commands._renewal_environment",
        return_value={"binding": "test"},
    ):
        result = runner.invoke(["ai", "claude", "--agent-args", "--settings", "{}"])
    assert result.exit_code != 0
    mock_run.assert_not_called()


def test_public_account_hostname_uses_documented_dashed_alias(mock_connection):
    from snowflake.cli._plugins.ai.commands import _derive_base_url

    connection = mock_connection.return_value[0]
    connection.host = "ORG-ACCOUNT_NAME.snowflakecomputing.com"
    url, error = _derive_base_url(connection, "SNOWFLAKE")
    assert error is None
    assert (
        url
        == "https://org-account-name.snowflakecomputing.com/api/v2/aigateways/SNOWFLAKE/"
    )


@pytest.mark.parametrize("authenticator", [PAT_AUTHENTICATOR, "oauth"])
@pytest.mark.parametrize("contents", ["file-credential", "", None])
def test_token_file_takes_precedence_without_fallback(
    mock_connection_context, tmp_path, authenticator, contents
):
    from snowflake.cli._plugins.ai.commands import _configured_bearer

    token_file = tmp_path / "token"
    if contents is not None:
        token_file.write_text(contents)
    with mock_connection_context(
        authenticator=authenticator,
        token="inline-credential",
        token_file_path=token_file,
    ):
        token, error = _configured_bearer()
    if contents:
        assert token.value == contents
        assert error is None
    else:
        assert token is None
        assert error


def test_agent_warnings_leave_stdout_clean(capsys):
    from snowflake.cli._plugins.ai.commands import (
        _warn_claude_models_unavailable,
        _warn_static_oauth,
    )

    _warn_static_oauth()
    _warn_claude_models_unavailable()
    output = capsys.readouterr()
    assert output.out == ""
    assert "not refreshed" in output.err
    assert "503" in output.err
