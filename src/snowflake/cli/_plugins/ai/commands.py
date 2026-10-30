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
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import click
import typer
from snowflake.cli._plugins.ai import oauth
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.secret import SecretType
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.network import (
    OAUTH_AUTHENTICATOR,
    OAUTH_AUTHORIZATION_CODE,
    PROGRAMMATIC_ACCESS_TOKEN,
)
from typer.core import TyperCommand

app = SnowTyperFactory(
    name="ai",
    help="Launches AI coding agents wired up to Snowflake.",
    # Hidden from the general command list: `snow --help` advertises GA commands, and
    # this group is in public preview. Discoverable via `snow ai --help` once known.
    is_hidden=lambda: True,
)

DEFAULT_GATEWAY_NAME = "SNOWFLAKE"
GATEWAY_PATH_TEMPLATE = "/api/v2/aigateways/{name}/"
# The gateway name is interpolated into a URL path, so it is restricted to the
# characters a Snowflake identifier can contain. Without this a name like
# "../../.." or one carrying a scheme would let a caller reshape the address the
# credential is sent to - which is the whole reason this is a name and not a URL.
GATEWAY_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_$]+$")

# Everything after this flag belongs to the launched agent.
AGENT_ARGS_FLAG = "--agent-args"


class AgentArgsCommand(TyperCommand):
    """Hands everything after the first AGENT_ARGS_FLAG to the agent, verbatim.

    Splitting before click parses is what makes this collision-proof. snow and the
    coding agents share a lot of flag names - --debug, --verbose, --model, --port,
    --host, --token, -v, -c - and snow's global connection options alone account for
    around forty of them. Anything handed to the parser is at risk of being claimed
    by snow, so the tail is never handed over at all: it is sliced off argv, click
    parses only the head, and the tail is assigned straight to ctx.args. No list of
    known-colliding flags to maintain, and a new snow global option cannot change
    what the agent receives.

    Only the first occurrence delimits, so a later --agent-args is forwarded like any
    other token.
    """

    def parse_args(self, ctx: click.Context, args: List[str]) -> List[str]:
        if AGENT_ARGS_FLAG in args:
            cut = args.index(AGENT_ARGS_FLAG)
            head, tail = args[:cut], args[cut + 1 :]
        else:
            head, tail = args, []

        try:
            super().parse_args(ctx, head)
        except click.UsageError as err:
            # Covers both an unrecognised option and a leftover positional, the
            # latter being what a bare `--` now produces. Either way the user most
            # likely meant the argument for the agent, so name the fix.
            raise self._agent_args_hint(ctx, err) from err

        # super() set ctx.args from whatever the head left over; with extra args
        # disallowed that is always empty, so this is an assignment, not an append.
        ctx.args = list(tail)
        return ctx.args

    def _agent_args_hint(
        self, ctx: click.Context, err: click.UsageError
    ) -> click.UsageError:
        """Rewrites a parse failure to point at AGENT_ARGS_FLAG.

        The hint has to come last, so click's own text is folded in first.
        NoSuchOption.format_message() appends "(Possible options: ...)" to whatever
        message it holds, which would otherwise land after the example invocation and
        read as part of it. Taking the formatted message and clearing the
        possibilities keeps click's suggestion, in its own words, ahead of ours.
        """
        message = err.format_message()
        if isinstance(err, click.NoSuchOption):
            err.possibilities = []
        invocation = " ".join(
            part for part in (ctx.command_path, AGENT_ARGS_FLAG, "...") if part
        )
        err.message = (
            f"{message}\n\n"
            f"If that argument was meant for {self.name}, put it after "
            f"{AGENT_ARGS_FLAG}, which forwards everything following it verbatim:\n"
            f"    {invocation}"
        )
        return err


# Telemetry settings requested of Claude Code.
#
# Claude Code needs an active trace exporter to propagate traceparent. Console is
# only the fallback when OTEL_TRACES_EXPORTER is unset; an explicit exporter wins.
# Propagating trace context on inference requests is separate from exporting client
# spans to the gateway. No OTLP endpoint or exporter credentials are set here.
# Only requested when telemetry is enabled for the connection.
#
# CLAUDE_CODE_ENHANCED_TELEMETRY_BETA broadens the emitted attributes; its exact
# payload has not been characterised, and doing so is a prerequisite for enabling this
# outside a dogfooding audience (see docs/ai-gateway.md).
#
# CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS was carried over from sf-cli's launcher, on
# the understanding that beta API features Claude Code negotiates are not implemented
# by the gateway. That reason is inherited rather than established here, and turning a
# feature off inside someone else's product needs one we can stand behind, so it is on
# the list to confirm with the gateway team or drop.
CLAUDE_TELEMETRY_ENV = {
    "CLAUDE_CODE_ENABLE_TELEMETRY": "1",
    "CLAUDE_CODE_ENHANCED_TELEMETRY_BETA": "1",
    "CLAUDE_CODE_PROPAGATE_TRACEPARENT": "1",
    "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS": "1",
}

DEFAULT_TRACES_EXPORTER = "console"

PAT_AUTHENTICATOR = PROGRAMMATIC_ACCESS_TOKEN

GATEWAY_NAME_OPTION = typer.Option(
    DEFAULT_GATEWAY_NAME,
    "--gateway-name",
    help="Name of the AI Gateway to route through. The address is always built from "
    "the account host of the active connection, so a private link connection reaches "
    "its own endpoint without further configuration.",
)

# Attribution headers sent on every request.
#
# These are client-supplied and unauthenticated: any caller can send the same values
# without going near this CLI, so they identify traffic that cooperates, not traffic
# that is verified. Treat them as a usage signal only - they must not back quota,
# billing, routing or any other enforcement decision.
#
# X-Snowflake-Application is the header sf-cli already sends
# (dev-env/sf-cli/shared/telemetry/claude_headers.go), so the launcher is legible to the
# same gateway-side tooling. Which agent was launched is carried by AGENT_NAME_HEADER.
APPLICATION_HEADER = "X-Snowflake-Application"
APPLICATION_NAME = "snowflake-cli"

# Distinguishes the two agents behind one application name; this spelling is the one
# the gateway asked for.
AGENT_NAME_HEADER = "snow-agent-name"
CLAUDE_AGENT_NAME = "claude"
OPENCODE_AGENT_NAME = "opencode"

# OpenCode is config-driven rather than env-driven: the gateway is wired up through
# provider.<provider>.options.{baseURL,apiKey}. OPENCODE_CONFIG_CONTENT supplies that
# config inline, so nothing is written to disk and the user's own config files are
# left alone. The injected config supplies the provider, model defaults and trace
# settings without supplying agents or MCP servers.
#
# Not the last word, though: OpenCode merges OPENCODE_CONFIG_CONTENT after the global,
# OPENCODE_CONFIG, project and OPENCODE_CONFIG_DIR sources - so none of those can
# override the provider block - but its console org config and managed preferences are
# merged afterwards and could.
OPENCODE_CONFIG_ENV = "OPENCODE_CONFIG_CONTENT"
# OpenCode ships a first-class Snowflake provider, so drive that rather than borrowing
# its Anthropic one. Two reasons this matters:
#
#   1. Auth. @ai-sdk/anthropic authenticates the Anthropic way - an x-api-key header
#      and no Authorization header - which the gateway rejects with 401. The
#      snowflake-cortex provider is @ai-sdk/openai-compatible and sends
#      `Authorization: Bearer <token>`, which is what the gateway expects.
#   2. Wire quirks. snowflake-cortex carries Snowflake-specific compatibility shims
#      we would otherwise have to reimplement: it rewrites max_tokens to
#      max_completion_tokens, repairs `"role":""` in streamed deltas, and turns a
#      400 "conversation complete" into a clean stop.
#
# Its own default baseURL points at Cortex REST (/api/v2/cortex/v1), which bypasses
# the gateway entirely; overriding options.baseURL is what routes it back through us.
OPENCODE_PROVIDER = "snowflake-cortex"
OPENCODE_SCHEMA = "https://opencode.ai/config.json"
# The API version belongs in the base URL: the Vercel AI SDK appends only the
# operation path (/chat/completions) to whatever baseURL it is given, exactly as
# OpenAI's own SDK does (https://api.openai.com/v1). Anthropic's official SDK is the
# outlier, which is why `snow ai claude` passes a bare host and this command does not.
OPENCODE_BASE_URL_SUFFIX = "v1"
# OpenCode runs a second, smaller model for thread titles, defaulting to one the
# gateway may not serve; an unavailable small model fails independently of the main
# one. Both are pinned to this default so a working session does not depend on the
# title model. These are config defaults rather than arguments, so OpenCode's own
# `-m provider/model` still wins for the main model while the title model stays put.
OPENCODE_DEFAULT_MODEL = "openai-gpt-5.4"
# Shown before OpenCode starts. The gateway does not currently serve Claude models to
# OpenCode (they fail with a 503), which the default model silently works around, so
# say so while the terminal is still ours - once the TUI starts it owns the screen.
OPENCODE_CLAUDE_NOTICE_SECONDS = 4

# The official installer drops the binary here and adds it to PATH via a shell rc,
# so a non-login shell may not see it.
OPENCODE_FALLBACK_BIN = Path.home() / ".opencode" / "bin" / "opencode"


@app.callback()
def ai():
    """Keeps `ai` a command group.

    Without a callback, typer collapses a single-subcommand Typer into that
    subcommand, which would expose it as `snow claude` instead of `snow ai claude`.
    """


def _derive_base_url(
    connection, gateway_name: str
) -> Tuple[Optional[str], Optional[str]]:
    """Builds the gateway base URL from the connection's account host.

    connection.host is the account endpoint we authenticated against, so the gateway
    always resolves to the same account as the token, and a private link connection
    resolves to its own endpoint. The host is never caller-supplied, which is what
    keeps the credential from being addressable anywhere else.

    Returns (base_url, error).
    """
    host = getattr(connection, "host", None)
    if not host:
        return None, "connection did not expose a host"
    if re.fullmatch(r"[A-Za-z0-9_-]+\.snowflakecomputing\.(com|cn)", host):
        host = host.replace("_", "-")
    path = GATEWAY_PATH_TEMPLATE.format(name=gateway_name)
    # Lowercased to match the documented account-URL construction; hostnames are
    # case-insensitive, so this is cosmetic.
    return f"https://{host.rstrip('/').lower()}{path}", None


def _resolve_connection() -> Tuple[Optional[object], Optional[str]]:
    """Opens (or reuses) the connection using the CLI's standard mechanism."""
    try:
        return get_cli_context().connection, None
    except Exception:
        return (
            None,
            "Check authentication and network settings with snow connection test",
        )


def _effective_connection_context():
    """Returns the connection parameters as they will actually be used.

    Uses the same typed flag/config/environment resolution as normal connections.
    Retains configured options so renewal can validate authentication and transport.
    """
    from snowflake.cli._app.snow_connector import resolve_connection_parameters

    context = get_cli_context().connection_context.clone()
    context.validate_and_complete()
    parameters = resolve_connection_parameters(**context.present_values_as_dict())
    for name, value in parameters.items():
        setattr(context, name, value)
    return context


def _configured_authenticator() -> Tuple[str, Optional[str]]:
    """Returns the authenticator declared for the active connection, lowercased.

    The connection config is the CLI's own source of truth for how the user
    authenticates, so it decides which credential the gateway gets. Lowercased to
    match how the CLI compares authenticators elsewhere (see _app/snow_connector.py).

    Returns (authenticator, error). A failure to read the configuration is reported
    rather than folded into the empty string, so the user is not told their
    authenticator is unset when the real problem is that the config could not be read.
    """
    try:
        authenticator = _effective_connection_context().authenticator
    except Exception:
        return "", "Check the selected connection's TOML and environment settings"
    return str(authenticator or "").lower(), None


def _configured_bearer() -> Tuple[Optional[SecretType], Optional[str]]:
    """Read a PAT or raw OAuth token from the connection configuration.

    A PAT is a documented CLI connection parameter (token / token_file_path, see
    _app/snow_connector.py), so this needs no connector internals, and the REST layer
    behind the gateway accepts it directly as `Authorization: Bearer`.

    Returns (token, error).
    """
    try:
        context = _effective_connection_context()
    except Exception:  # pragma: no cover - defensive
        return None, "could not resolve the selected connection configuration"

    token_file_path = context.token_file_path
    if token_file_path:
        path = Path(token_file_path)
        try:
            token = (
                SecurePath(path)
                .read_text(file_size_limit_mb=1, encoding="utf-8")
                .strip()
            )
        except Exception:
            return (
                None,
                "could not read token_file_path; check its encoding and permissions",
            )
        if token:
            return SecretType(token), None
        return None, "token_file_path is empty"

    if context.token:
        return SecretType(context.token), None

    return None, "no token or token_file_path is configured for this connection"


def _cached_oauth_bearer() -> SecretType:
    """Read the selected login's access token using private connector interfaces.

    The connector clears assertion_content after login. Its authenticator retains
    the cache and issuer, but not the user, so use the authenticated connection's
    user. Never construct a separate cache or substitute a driver session token.
    This adapter must be revalidated when the connector changes.
    """
    try:
        from snowflake.connector.auth.oauth_code import AuthByOauthCode
        from snowflake.connector.token_cache import TokenKey, TokenType
    except ImportError:
        raise CliError(
            "This connector version does not expose the OAuth credential cache "
            "required by snow ai. Use a supported connector or a PAT connection."
        ) from None

    connection, _ = _resolve_connection()
    if connection is None:
        raise CliError("Could not resolve the authenticated OAuth connection.")
    auth = getattr(connection, "auth_class", None)
    if not isinstance(auth, AuthByOauthCode):
        raise CliError(
            "The connection did not expose the expected OAuth authorization-code "
            "authenticator. Check the connector version and connection configuration."
        )
    cache = getattr(auth, "_token_cache", None)
    issuer = getattr(auth, "_idp_host", None)
    user = getattr(connection, "user", None)
    if cache is None:
        raise CliError(
            "OAuth credential caching is unavailable. Set "
            "client_store_temporary_credential = true for this connection and "
            "ensure the connector's secure local storage is available, then relaunch."
        )
    if (
        not isinstance(issuer, str)
        or not issuer
        or not isinstance(user, str)
        or not user
    ):
        raise CliError(
            "Cannot identify the current OAuth credential cache entry. "
            "Check the connector version and sign in again."
        )
    try:
        token = cache.retrieve(
            TokenKey(user=user, host=issuer, tokenType=TokenType.OAUTH_ACCESS_TOKEN)
        )
    except Exception:
        raise CliError(
            "Could not read the OAuth access-token cache. Check secure local "
            "storage access and sign in again."
        ) from None
    if not isinstance(token, str) or not token.strip():
        raise CliError(
            "No OAuth access token was cached by this login. Ensure "
            "client_store_temporary_credential = true and secure local storage "
            "is available, then sign in again and relaunch."
        )
    return SecretType(token)


def _resolve_bearer_token() -> SecretType:
    """Returns the credential the AI Gateway accepts, or refuses to continue.

    The gateway sits behind Snowflake's REST layer, which accepts a bearer credential
    but rejects the driver session token with an empty-bodied 401. Rather than passing
    a token that is known to fail, a connection that cannot supply one is refused here
    with a message naming what is supported.

    PAT and raw OAuth use static configured credentials. Authorization-code OAuth
    checks the connector cache after login and installs a renewal helper at launch.
    """
    authenticator, config_error = _configured_authenticator()

    if config_error is not None:
        raise CliError(
            f"Could not read the connection configuration: {config_error}.\n"
            "The gateway credential comes from the connection, so fix the connection "
            "first (see `snow connection test`)."
        )

    if authenticator in {PAT_AUTHENTICATOR.lower(), OAUTH_AUTHENTICATOR.lower()}:
        token, error = _configured_bearer()
        if token is None:
            raise CliError(
                f"Connection is configured for {authenticator} but {error}.\n"
                "Set token (or token_file_path) for the connection and try again."
            )
        if authenticator == OAUTH_AUTHENTICATOR.lower():
            _warn_static_oauth()
        return token

    if authenticator == OAUTH_AUTHORIZATION_CODE.lower():
        token = _cached_oauth_bearer()
        return token

    raise CliError(
        "Could not derive a credential for the AI Gateway from this connection "
        f"(authenticator: {authenticator or 'not set'}).\n"
        f"Configure a programmatic access token: authenticator = {PAT_AUTHENTICATOR} "
        "with token or token_file_path.\n"
        "OAuth is also supported: oauth with token or token_file_path, or "
        "oauth_authorization_code with credential caching enabled.\n"
        "Keypair (SNOWFLAKE_JWT), OAuth client credentials and workload identity "
        "are not supported yet."
    )


def _warn_static_oauth() -> None:
    cli_console.stderr_warning(
        "OAuth credentials are copied at launch and are not refreshed inside the "
        "agent. If the token expires, obtain a valid token and relaunch snow ai. "
        "Connector refresh settings do not renew a running agent's token."
    )


def _validate_gateway_name(gateway_name: str) -> str:
    """Returns the gateway name, or refuses one that could reshape the URL.

    The name is interpolated into the path of the address a live credential is sent
    to, so anything outside the Snowflake identifier character set is rejected rather
    than escaped: it is never legitimate, and quoting it would still leave the address
    partly caller-controlled.

    Case is preserved rather than normalised: the name addresses a Snowflake object,
    and folding it here would silently change which gateway is addressed if the
    gateway resolves names case-sensitively. Callers get exactly what they typed.
    """
    if not GATEWAY_NAME_PATTERN.match(gateway_name or ""):
        raise CliError(
            f"Invalid gateway name {gateway_name!r}.\n"
            "A gateway name may contain only letters, digits, underscores and dollar "
            f"signs (default: {DEFAULT_GATEWAY_NAME}). The gateway address is always "
            "built from the account host of the active connection."
        )
    return gateway_name


def _resolve_gateway(
    gateway_name: str = DEFAULT_GATEWAY_NAME,
) -> Tuple[str, SecretType]:
    """Resolves the gateway URL and bearer token for the active connection.

    Both come from the same connection, so the credential and the address it is sent
    to cannot point at different accounts. Raises CliError with an actionable message
    rather than returning a half-configured result.
    """
    gateway_name = _validate_gateway_name(gateway_name)

    connection, connection_error = _resolve_connection()
    if connection is None:
        raise CliError(
            f"Could not connect: {connection_error}.\n"
            "The gateway credential is derived from the connection, so fix the "
            "connection first (see `snow connection test`)."
        )

    base_url, url_error = _derive_base_url(connection, gateway_name)
    if base_url is None:
        raise CliError(f"Could not derive the gateway URL: {url_error}.")

    return base_url, _resolve_bearer_token()


def _find_agent_binary(name: str, fallback: Optional[Path] = None) -> str:
    """Locates an agent binary on PATH, with an optional known install location.

    Worth stating plainly: whatever binary is first on PATH under this name receives a
    live Snowflake credential in its environment. Nothing here verifies its version,
    signature or provenance, and the fallback extends that to a user-writable path
    under $HOME. That is inherent to launching someone else's agent, but it is the
    last link in the chain and should not be discovered by reading the code.
    """
    found = shutil.which(name)
    if found:
        return found
    if fallback is not None and fallback.is_file() and os.access(fallback, os.X_OK):
        return str(fallback)
    raise CliError(
        f"{name} command not found, make sure it is installed on your system and in "
        "your PATH"
    )


def _telemetry_allowed() -> bool:
    """Whether telemetry is enabled for the active connection.

    snow's own telemetry rides on the connector's client (see
    _app/telemetry.py: connection._telemetry), which is gated by
    connection.telemetry_enabled - the client parameter combined with the server-side
    setting. Forcing a third-party tool's telemetry on for someone who turned snow's
    off would be a decision this command has no business making, so the same switch
    governs both. Defaults to enabled when the attribute is absent, and when the
    connection cannot be inspected at all - the launch itself fails first in that
    case, so this branch does not get to decide anything.
    """
    connection, _ = _resolve_connection()
    if connection is None:
        return True
    return bool(getattr(connection, "telemetry_enabled", True))


def _attribution_headers(agent_name: str) -> Dict[str, str]:
    """Returns the headers that identify this launcher and the agent it started.

    Unauthenticated by nature - see the note on APPLICATION_HEADER.
    """
    return {
        APPLICATION_HEADER: APPLICATION_NAME,
        AGENT_NAME_HEADER: agent_name,
    }


def _merge_custom_headers(existing: Optional[str], headers: Dict[str, str]) -> str:
    """Adds headers to an ANTHROPIC_CUSTOM_HEADERS value, first-write-wins.

    The format is "Name: Value", newline-separated. A caller or parent process may
    already have populated this variable - sf-cli writes its own attribution headers
    into it - so existing entries are preserved and only missing names are appended.
    Comparison is case-insensitive because header names are.
    """
    lines = [line for line in (existing or "").splitlines() if line.strip()]
    present = {line.split(":", 1)[0].strip().lower() for line in lines if ":" in line}
    for name, value in headers.items():
        if name.lower() not in present:
            lines.append(f"{name}: {value}")
    return "\n".join(lines)


# Environment variables that would silently replace the credential we resolved, or
# route the agent somewhere other than the gateway. Using a credential other than the
# one whose authenticator was just validated is the worst available outcome, so these
# are refused rather than overridden - the agent reads some of them in preference to
# what we pass, so overriding is not reliably possible anyway.
CLAUDE_CONFLICTING_ENV = (
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
)
OPENCODE_CONFLICTING_ENV = (
    # SNOWFLAKE_ACCOUNT is deliberately absent: it is a documented snow connection
    # variable (api/config_ng/sources.py, CliEnvironment), so refusing it would break
    # a supported way of configuring the very connection this command depends on. It
    # also cannot change the credential or the endpoint - the provider uses account
    # only to interpolate a default baseURL that options.baseURL overrides.
    "SNOWFLAKE_CORTEX_TOKEN",
    "SNOWFLAKE_CORTEX_PAT",
)


def _refuse_conflicting_env(names: Tuple[str, ...]) -> None:
    """Fails when the environment would change which credential or endpoint is used."""
    found = [name for name in names if os.environ.get(name)]
    if not found:
        return
    listed = ", ".join(found)
    raise CliError(
        f"Refusing to launch: {listed} is set in the environment.\n"
        "The agent reads these in preference to the settings this command passes, so "
        "it would authenticate with a credential that was never validated, or send "
        "traffic somewhere other than the AI Gateway.\n"
        f"Unset {listed} and try again."
    )


def _build_env(base_url: str, token: SecretType) -> Dict[str, Union[str, SecretType]]:
    """Layers our overrides on top of the caller's environment.

    The user's own settings win wherever they express an intent we should not
    contradict: an existing OTEL exporter, and any telemetry variable they set
    deliberately. Telemetry is only requested at all when it is enabled for the
    connection. The token remains wrapped until the subprocess handoff so formatting
    this mapping does not expose it.
    """
    env: Dict[str, Union[str, SecretType]] = dict(os.environ)
    if _telemetry_allowed():
        for name, value in CLAUDE_TELEMETRY_ENV.items():
            env.setdefault(name, value)
        env.setdefault("OTEL_TRACES_EXPORTER", DEFAULT_TRACES_EXPORTER)
    env["ANTHROPIC_BASE_URL"] = base_url
    env["ANTHROPIC_AUTH_TOKEN"] = token
    env["ANTHROPIC_CUSTOM_HEADERS"] = _merge_custom_headers(
        os.environ.get("ANTHROPIC_CUSTOM_HEADERS"),
        _attribution_headers(CLAUDE_AGENT_NAME),
    )
    return env


def _renewal_environment() -> Optional[Dict[str, str]]:
    authenticator, _ = _configured_authenticator()
    if authenticator != OAUTH_AUTHORIZATION_CODE.lower():
        return None
    connection, _ = _resolve_connection()
    binding = oauth.binding_for(_effective_connection_context(), connection)
    return {
        oauth.BINDING_ENV: json.dumps(binding),
        "SNOWFLAKE_AI_OAUTH_COMMAND": json.dumps(oauth.helper_argv()),
    }


@app.command(
    name="claude",
    requires_connection=True,
    cls=AgentArgsCommand,
    # The terminator hands super().parse_args an empty head whenever every argument
    # is the agent's, e.g. `snow ai claude --agent-args -p hi`. click prints help and
    # exits on an empty arg list when this is true, which would swallow the tail, so
    # it is pinned rather than left to click's default.
    no_args_is_help=False,
    epilog=f"Pass arguments to the agent after {AGENT_ARGS_FLAG}, for example:\n\n"
    f"    snow ai claude {AGENT_ARGS_FLAG} -p 'explain this repo' --verbose",
)
def claude(
    ctx: typer.Context,
    gateway_name: str = GATEWAY_NAME_OPTION,
    **options,
):
    """Launches Claude Code with the environment variables the AI Gateway requires.

    Arguments for the `claude` binary go after --agent-args, which forwards
    everything following it verbatim:

        snow ai claude --agent-args -p 'explain this repo' --verbose

    The terminator exists because snow and claude share many flag names (--debug,
    --verbose, --model, -v). Anything before it belongs to snow, anything after it
    belongs to claude, so neither can shadow the other.

    Your Claude Code settings file is never modified; these variables apply only to
    the launched process.
    """
    claude_bin = _find_agent_binary("claude")
    _refuse_conflicting_env(CLAUDE_CONFLICTING_ENV)
    # Anthropic's SDK appends /v1/messages itself, so the base URL stays version-less.
    base_url, token = _resolve_gateway(gateway_name)

    env = _build_env(base_url, token)
    claude_args: List[str] = list(ctx.args)
    renewal = _renewal_environment()
    if renewal:
        if (
            "--safe-mode" in claude_args
            or os.environ.get("CLAUDE_CODE_SAFE_MODE") == "1"
        ):
            raise CliError(
                "Claude safe mode disables customizations and is not supported with the OAuth helper."
            )
        if any(
            argument == "--settings" or argument.startswith("--settings=")
            for argument in claude_args
        ):
            raise CliError(
                "OAuth helper settings cannot be combined with native --settings yet."
            )
        env.update(renewal)
        env.pop("ANTHROPIC_AUTH_TOKEN", None)
        env.pop("CLAUDE_CODE_OAUTH_TOKEN", None)
        claude_args = [
            "--settings",
            json.dumps(
                {
                    "apiKeyHelper": shlex.join(oauth.helper_argv()),
                    # Claude settings.env can override the inherited environment.
                    "env": {"ANTHROPIC_BASE_URL": base_url},
                }
            ),
            *claude_args,
        ]

    result = subprocess.run(
        [claude_bin, *claude_args],
        env={
            name: value.value if isinstance(value, SecretType) else value
            for name, value in env.items()
        },
    )
    if result.returncode:
        raise typer.Exit(code=result.returncode)


def _opencode_account(base_url: str) -> str:
    """Returns the account value OpenCode's Snowflake provider needs to configure itself.

    The provider builds its options - including the compat fetch wrapper that rewrites
    max_tokens to max_completion_tokens, repairs `"role":""` in streamed deltas and turns
    a 400 "conversation complete" into a clean stop - only after a guard that returns
    early when no account is resolved. So without an account the wrapper is never
    attached. Confirmed on the wire: the request then goes out with a raw max_tokens and
    the gateway rejects it, and because OpenCode falls back to a plain
    openai-compatible client built from these options it surfaces as a model
    compatibility error rather than as missing credentials.

    It is not read as a Snowflake account identifier. The provider only uses it to
    interpolate a default baseURL of https://<account>.snowflakecomputing.com, which
    options.baseURL then overrides, so the host already being addressed is the correct
    value. Note that SNOWFLAKE_ACCOUNT, SNOWFLAKE_CORTEX_TOKEN and SNOWFLAKE_CORTEX_PAT
    in the environment take precedence over what is passed here.
    """
    return urlparse(base_url).hostname or base_url


def _build_opencode_config(base_url: str, token: SecretType) -> SecretType:
    """Builds the inline OpenCode config that points the provider at the gateway.

    Supplies the provider and model defaults. Telemetry plugins and exporter
    settings are left to the user; no external package is implicitly loaded.

    Returned as a SecretType because the JSON embeds the credential: a stray log line
    or traceback should render it masked rather than print an account credential.
    """
    options: Dict[str, Any] = {
        "baseURL": base_url,
        "account": _opencode_account(base_url),
        "apiKey": token.value,
        # Reach the wire as request headers, so gateway traffic is legible as having
        # come from this launcher. Unauthenticated - see APPLICATION_HEADER.
        "headers": _attribution_headers(OPENCODE_AGENT_NAME),
    }
    qualified_model = f"{OPENCODE_PROVIDER}/{OPENCODE_DEFAULT_MODEL}"
    config: Dict[str, Any] = {
        "$schema": OPENCODE_SCHEMA,
        "model": qualified_model,
        # Pinned to the same model so the title request cannot pick something the
        # gateway does not serve, even when the main model is overridden natively.
        "small_model": qualified_model,
        "provider": {
            OPENCODE_PROVIDER: {
                "options": options,
                "models": {
                    OPENCODE_DEFAULT_MODEL: {
                        # GPT-5.4 chat completions reject tools with reasoning enabled.
                        "options": {"reasoningEffort": "none"},
                    }
                },
            }
        },
    }
    return SecretType(json.dumps(config))


def _is_interactive() -> bool:
    """Whether stdout is a terminal a human is likely watching.

    Wrapped rather than calling sys.stdout.isatty() inline because sys.stdout is
    swapped out by test runners and by shell redirection, so the check has to happen
    at call time on whatever stdout currently is.
    """
    return sys.stdout.isatty()


def _warn_claude_models_unavailable() -> None:
    """Warns that the gateway does not serve Claude models to OpenCode.

    Unconditional rather than conditional on the requested model: working that out
    would mean parsing the forwarded argument tail for -m/--model and resolving it
    against OpenCode's own config precedence, which is exactly the argument
    inspection AGENT_ARGS_FLAG exists to avoid.

    The pause only happens on a TTY, so scripted and CI runs are not delayed.
    """
    cli_console.stderr_warning(
        "Claude models are not currently served through the AI Gateway for OpenCode "
        f"and fail with a 503. Defaulting to {OPENCODE_DEFAULT_MODEL}.\n"
        "Override with: snow ai opencode "
        f"{AGENT_ARGS_FLAG} -m {OPENCODE_PROVIDER}/<model>"
    )
    if _is_interactive():
        time.sleep(OPENCODE_CLAUDE_NOTICE_SECONDS)


@app.command(
    name="opencode",
    requires_connection=True,
    cls=AgentArgsCommand,
    # See the note on the claude command: an all-agent invocation leaves the head
    # empty, and click would print help instead of launching.
    no_args_is_help=False,
    epilog=f"Pass arguments to the agent after {AGENT_ARGS_FLAG}, for example:\n\n"
    f"    snow ai opencode {AGENT_ARGS_FLAG} run 'fix the failing test'",
)
def opencode(
    ctx: typer.Context,
    gateway_name: str = GATEWAY_NAME_OPTION,
    **options,
):
    """Launches OpenCode with its Snowflake provider pointed at the AI Gateway.

    Arguments for the `opencode` binary go after --agent-args, which forwards
    everything following it verbatim:

        snow ai opencode --agent-args run 'fix the failing test'

    The terminator exists because snow and opencode share many flag names (--debug,
    --model, --port, -v). Anything before it belongs to snow, anything after it
    belongs to opencode, so neither can shadow the other. OpenCode's own flags are
    the only ones for driving it, including the model:

        snow ai opencode --agent-args -m snowflake-cortex/<model>

    Your OpenCode config files are never modified. The gateway settings are passed
    inline via OPENCODE_CONFIG_CONTENT and merged with your own config for this
    process only.
    """
    opencode_bin = _find_agent_binary("opencode", OPENCODE_FALLBACK_BIN)
    _refuse_conflicting_env(OPENCODE_CONFLICTING_ENV)
    base_url, token = _resolve_gateway(gateway_name)
    # The SDK appends only the operation path, so the version has to be in the base URL.
    base_url = f"{base_url.rstrip('/')}/{OPENCODE_BASE_URL_SUFFIX}"

    env = dict(os.environ)
    # Unwrapped at the single point of use; see _build_opencode_config.
    config = json.loads(_build_opencode_config(base_url, token).value)
    renewal = _renewal_environment()
    if renewal:
        if any(
            argument in {"--pure", "--attach"} or argument.startswith("--attach=")
            for argument in ctx.args
        ) or os.environ.get("OPENCODE_PURE", "").lower() in {"1", "true"}:
            raise CliError(
                "OpenCode --pure and --attach cannot be used with the local OAuth plugin."
            )
        env.update(renewal)
        env["SNOWFLAKE_AI_OAUTH_GATEWAY"] = base_url
        config["provider"][OPENCODE_PROVIDER]["options"][
            "apiKey"
        ] = "snow-cli-oauth-helper-required"
        config.setdefault("plugin", []).append(
            Path(__file__).with_name("opencode_oauth.mjs").resolve().as_uri()
        )
    env[OPENCODE_CONFIG_ENV] = json.dumps(config)
    opencode_args: List[str] = list(ctx.args)

    _warn_claude_models_unavailable()

    result = subprocess.run([opencode_bin, *opencode_args], env=env)
    if result.returncode:
        raise typer.Exit(code=result.returncode)
