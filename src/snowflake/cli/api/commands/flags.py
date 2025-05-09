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

from pathlib import Path
from typing import Any, Callable, Optional

import click
import typer
from click import ClickException
from snowflake.cli.api.cli_global_context import (
    _CliGlobalContextManager,
    get_cli_context_manager,
)
from snowflake.cli.api.commands.common import OnErrorType
from snowflake.cli.api.commands.overrideable_parameter import OverrideableOption
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.config import get_all_connections
from snowflake.cli.api.connections import ConnectionContext
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.secret import SecretType
from snowflake.cli.api.stage_path import StagePath

DEFAULT_CONTEXT_SETTINGS = {"help_option_names": ["--help", "-h"]}

_CONNECTION_SECTION = "Connection configuration"
_CLI_BEHAVIOUR = "Global configuration"


def _connection_callback(prop: str):
    """Generates a setter for a field on the current context manager's connection context."""
    if prop not in ConnectionContext.__dataclass_fields__:
        raise KeyError(
            f"Cannot generate setter for non-existent connection attr {prop}"
        )

    def callback(value):
        try:
            if click.get_current_context().resilient_parsing:
                return
        except RuntimeError:
            pass

        setattr(get_cli_context_manager().connection_context, prop, value)
        return value

    return callback


def _context_callback(prop: str):
    """Generates a setter for a field on the current context manager."""
    if prop not in _CliGlobalContextManager.__dataclass_fields__:
        raise KeyError(f"Cannot generate setter for non-existent context attr {prop}")

    def callback(value):
        try:
            if click.get_current_context().resilient_parsing:
                return
        except RuntimeError:
            pass

        setattr(get_cli_context_manager(), prop, value)
        return value

    return callback


ConnectionOption = typer.Option(
    None,
    "--connection",
    "-c",
    "--environment",
    help=f"Name of the connection, as defined in your `config.toml` file. Default: `default`.",
    callback=_connection_callback("connection_name"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    shell_complete=lambda _, __, ___: list(get_all_connections()),
)

TemporaryConnectionOption = typer.Option(
    False,
    "--temporary-connection",
    "-x",
    help="Uses a connection defined with command line parameters, instead of one defined in config",
    callback=_connection_callback("temporary_connection"),
    is_flag=True,
    rich_help_panel=_CONNECTION_SECTION,
)

HostOption = typer.Option(
    None,
    "--host",
    help="Host address for the connection. Overrides the value specified for the connection.",
    callback=_connection_callback("host"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

PortOption = typer.Option(
    None,
    "--port",
    help="Port for the connection. Overrides the value specified for the connection.",
    callback=_connection_callback("port"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

AccountOption = typer.Option(
    None,
    "--account",
    "--accountname",
    help="Name assigned to your Snowflake account. Overrides the value specified for the connection.",
    callback=_connection_callback("account"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

UserOption = typer.Option(
    None,
    "--user",
    "--username",
    help="Username to connect to Snowflake. Overrides the value specified for the connection.",
    callback=_connection_callback("user"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)


PLAIN_PASSWORD_MSG = "WARNING! Using --password via the CLI is insecure. Use environment variables instead."


def _password_callback(value: str):
    if value:
        cli_console.message(PLAIN_PASSWORD_MSG)

    return _connection_callback("password")(value)


PasswordOption = typer.Option(
    None,
    "--password",
    help="Snowflake password. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_password_callback,
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

AuthenticatorOption = typer.Option(
    None,
    "--authenticator",
    help="Snowflake authenticator. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_connection_callback("authenticator"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

PrivateKeyPathOption = typer.Option(
    None,
    "--private-key-file",
    "--private-key-path",
    help="Snowflake private key file path. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_connection_callback("private_key_file"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    file_okay=True,
    dir_okay=False,
)

SessionTokenOption = typer.Option(
    None,
    "--session-token",
    help="Snowflake session token. Can be used only in conjunction with --master-token. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_connection_callback("session_token"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    file_okay=True,
    dir_okay=False,
    hidden=True,
)

MasterTokenOption = typer.Option(
    None,
    "--master-token",
    help="Snowflake master token. Can be used only in conjunction with --session-token. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_connection_callback("master_token"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    file_okay=True,
    dir_okay=False,
    hidden=True,
)

TokenFilePathOption = typer.Option(
    None,
    "--token-file-path",
    help="Path to file with an OAuth token to use when connecting to Snowflake.",
    callback=_connection_callback("token_file_path"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    file_okay=True,
    dir_okay=False,
)

DatabaseOption = typer.Option(
    None,
    "--database",
    "--dbname",
    help="Database to use. Overrides the value specified for the connection.",
    callback=_connection_callback("database"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

SchemaOption = typer.Option(
    None,
    "--schema",
    "--schemaname",
    help="Database schema to use. Overrides the value specified for the connection.",
    callback=_connection_callback("schema"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

RoleOption = typer.Option(
    None,
    "--role",
    "--rolename",
    help="Role to use. Overrides the value specified for the connection.",
    callback=_connection_callback("role"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

WarehouseOption = typer.Option(
    None,
    "--warehouse",
    help="Warehouse to use. Overrides the value specified for the connection.",
    callback=_connection_callback("warehouse"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

MfaPasscodeOption = typer.Option(
    None,
    "--mfa-passcode",
    help="Token to use for multi-factor authentication (MFA)",
    callback=_connection_callback("mfa_passcode"),
    prompt="MFA passcode",
    prompt_required=False,
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

EnableDiagOption = typer.Option(
    False,
    "--enable-diag",
    help="Whether to generate a connection diagnostic report.",
    callback=_connection_callback("enable_diag"),
    show_default=False,
    is_flag=True,
    rich_help_panel=_CONNECTION_SECTION,
)

OauthClientIdOption = typer.Option(
    None,
    "--oauth-client-id",
    help="Value of client id provided by the Identity Provider for Snowflake integration.",
    callback=_connection_callback("oauth_client_id"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

OauthClientSecretOption = typer.Option(
    None,
    help="Value of the client secret provided by the Identity Provider for Snowflake integration.",
    callback=_connection_callback("oauth_client_secret"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

OauthAuthorizationUrlOption = typer.Option(
    None,
    "--oauth-authorization-url",
    help="Identity Provider endpoint supplying the authorization code to the driver.",
    callback=_connection_callback("oauth_authorization_url"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

OauthTokenRequestUrlOption = typer.Option(
    None,
    "--oauth-token-request-url",
    help="Identity Provider endpoint supplying the access tokens to the driver.",
    callback=_connection_callback("oauth_token_request_url"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

OauthRedirectUriOption = typer.Option(
    None,
    "--oauth-redirect-uri",
    help="URI to use for authorization code redirection.",
    callback=_connection_callback("oauth_redirect_uri"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

OauthScopeOption = typer.Option(
    None,
    "--oauth-scope",
    help="Scope requested in the Identity Provider authorization request.",
    callback=_connection_callback("oauth_scope"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

OauthDisablePkceOption = typer.Option(
    None,
    "--oauth-disable-pkce",
    help="Disables Proof Key for Code Exchange (PKCE). Default: `False`.",
    callback=_connection_callback("oauth_disable_pkce"),
    show_default=False,
    is_flag=True,
    rich_help_panel=_CONNECTION_SECTION,
)

OauthEnableRefreshTokensOption = typer.Option(
    None,
    "--oauth-enable-refresh-tokens",
    help="Enables a silent re-authentication when the actual access token becomes outdated. Default: `False`.",
    callback=_connection_callback("oauth_enable_refresh_tokens"),
    show_default=False,
    is_flag=True,
    rich_help_panel=_CONNECTION_SECTION,
)

OauthEnableSingleUseRefreshTokensOption = typer.Option(
    None,
    "--oauth-enable-single-use-refresh-tokens",
    help="Whether to opt-in to single-use refresh token semantics. Default: `False`.",
    callback=_connection_callback("oauth_enable_single_use_refresh_tokens"),
    show_default=False,
    is_flag=True,
    rich_help_panel=_CONNECTION_SECTION,
)

ClientStoreTemporaryCredentialOption = typer.Option(
    None,
    "--client-store-temporary-credential",
    help="Store the temporary credential.",
    callback=_connection_callback("client_store_temporary_credential"),
    is_flag=True,
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

# Set default via callback to avoid including tempdir path in generated docs (snow --docs).
# Use constant instead of None, as None is removed from telemetry data.
_DIAG_LOG_DEFAULT_VALUE = "<system_temporary_directory>"


def _diag_log_path_callback(path: str):
    if path == _DIAG_LOG_DEFAULT_VALUE:
        import tempfile

        path = tempfile.gettempdir()

    absolute_path = Path(path).absolute()
    if not absolute_path.exists():
        # if the path does not exist the report is not generated
        from snowflake.cli.api.secure_path import SecurePath

        SecurePath(absolute_path).mkdir(parents=True)

    return _connection_callback("diag_log_path")(absolute_path)


DiagLogPathOption: Path = typer.Option(
    _DIAG_LOG_DEFAULT_VALUE,
    "--diag-log-path",
    help="Path for the generated report. Defaults to system temporary directory.",
    callback=_diag_log_path_callback,
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    writable=True,
)


def _diag_log_allowlist_path_callback(path: str):
    absolute_path = Path(path).absolute() if path else None
    return _connection_callback("diag_allowlist_path")(absolute_path)


DiagAllowlistPathOption: Path = typer.Option(
    None,
    "--diag-allowlist-path",
    help="Path to a JSON file that contains allowlist parameters.",
    callback=_diag_log_allowlist_path_callback,
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    file_okay=True,
)

OutputFormatOption = typer.Option(
    OutputFormat.TABLE.value,
    "--format",
    help="Specifies the output format.",
    case_sensitive=False,
    callback=_context_callback("output_format"),
    rich_help_panel=_CLI_BEHAVIOUR,
)

SilentOption = typer.Option(
    False,
    "--silent",
    help="Turns off intermediate output to console.",
    callback=_context_callback("silent"),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
    is_eager=True,
)

VerboseOption = typer.Option(
    False,
    "--verbose",
    "-v",
    help="Displays log entries for log levels `info` and higher.",
    callback=_context_callback("verbose"),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)

DebugOption = typer.Option(
    False,
    "--debug",
    help="Displays log entries for log levels `debug` and higher; debug logs contain additional information.",
    callback=_context_callback("enable_tracebacks"),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)

EnhancedExitCodesOption = typer.Option(
    False,
    "--enhanced-exit-codes",
    help="Differentiate exit error codes based on failure type.",
    callback=_context_callback("enhanced_exit_codes"),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
    is_eager=True,
    envvar="SNOWFLAKE_ENHANCED_EXIT_CODES",
)

# If IfExistsOption, IfNotExistsOption, or ReplaceOption are used with names other than those in CREATE_MODE_OPTION_NAMES,
# you must also override mutually_exclusive if you want to retain the validation that at most one of these flags is
# passed.
CREATE_MODE_OPTION_NAMES = ["if_exists", "if_not_exists", "replace"]

IfExistsOption = OverrideableOption(
    False,
    "--if-exists",
    help="Only apply this operation if the specified object exists.",
    mutually_exclusive=CREATE_MODE_OPTION_NAMES,
)

IfNotExistsOption = OverrideableOption(
    False,
    "--if-not-exists",
    help="Only apply this operation if the specified object does not already exist.",
    mutually_exclusive=CREATE_MODE_OPTION_NAMES,
)

ReplaceOption = OverrideableOption(
    False,
    "--replace",
    help="Replace this object if it already exists.",
    mutually_exclusive=CREATE_MODE_OPTION_NAMES,
)

ForceReplaceOption = OverrideableOption(
    False,
    "--force-replace",
    help="Replace this object, even if the state didn't change",
)

OnErrorOption = typer.Option(
    OnErrorType.BREAK.value,
    "--on-error",
    help="What to do when an error occurs. Defaults to break.",
)

NoInteractiveOption = typer.Option(False, "--no-interactive", help="Disable prompting.")

PruneOption = OverrideableOption(
    False,
    "--prune/--no-prune",
    help=f"Delete files that exist in the stage, but not in the local filesystem.",
    show_default=True,
)


def entity_argument(entity_type: str, required=False) -> typer.Argument:
    _help = f"ID of {entity_type} entity."
    if not required:
        return typer.Argument(None, help=_help)
    return typer.Argument(..., help=_help, show_default=False)


def variables_option(description: str):
    return typer.Option(
        None,
        "--variable",
        "-D",
        help=description,
        show_default=False,
    )


ExecuteVariablesOption = variables_option(
    'Variables for the execution context; for example: `-D "<key>=<value>"`. '
    "For SQL files, variables are used to expand the template, and any unknown variable will cause an error (consider embedding quoting in the file)."
    "For Python files, variables are used to update the os.environ dictionary. Provided keys are capitalized to adhere to best practices. "
    "In case of SQL files string values must be quoted in `''` (consider embedding quoting in the file).",
)


def like_option(help_example: str):
    return typer.Option(
        "%%",
        "--like",
        "-l",
        help=f"SQL LIKE pattern for filtering objects by name. For example, {help_example}.",
    )


def _pattern_option_callback(value):
    if value and value.count("'") != value.count("\\'"):
        raise ClickException('All "\'" characters in PATTERN must be escaped: "\\\'"')
    return value


PatternOption = typer.Option(
    None,
    "--pattern",
    help=(
        "Regex pattern for filtering files by name."
        r' For example --pattern ".*\.txt" will filter only files with .txt extension.'
    ),
    show_default=False,
    callback=_pattern_option_callback,
)


def experimental_option(
    experimental_behaviour_description: Optional[str] = None,
) -> typer.Option:
    help_text = (
        f"Turns on experimental behaviour of the command: {experimental_behaviour_description}"
        if experimental_behaviour_description
        else "Turns on experimental behaviour of the command."
    )
    return typer.Option(
        False,
        "--experimental",
        help=help_text,
        hidden=True,
        callback=_context_callback("experimental"),
        is_flag=True,
        rich_help_panel=_CLI_BEHAVIOUR,
    )


class IdentifierType(click.ParamType):
    name = "TEXT"

    def convert(self, value, param, ctx):
        return FQN.from_string(value)


class IdentifierStageType(click.ParamType):
    name = "TEXT"

    def convert(self, value, param, ctx):
        return FQN.from_stage(value)


class IdentifierStagePathType(click.ParamType):
    name = "TEXT"

    def convert(self, value, param, ctx):
        return StagePath.from_stage_str(value)


class SecretTypeParser(click.ParamType):
    name = "TEXT"

    def convert(self, value, param, ctx):
        if not isinstance(value, SecretType):
            return SecretType(value)
        return value


def identifier_argument(
    sf_object: str,
    example: str,
    click_type: click.ParamType = IdentifierType(),
    callback: Callable | None = None,
    is_optional: bool = False,
) -> typer.Argument:
    return typer.Argument(
        None if is_optional else ...,
        help=f"Identifier of the {sf_object}; for example: {example}",
        show_default=False,
        click_type=click_type,
        callback=callback,
    )


def identifier_stage_argument(
    sf_object: str, example: str, callback: Callable | None = None
) -> typer.Argument:
    return identifier_argument(
        sf_object, example, click_type=IdentifierStageType(), callback=callback
    )


def identifier_stage_path_argument(
    sf_object: str, example: str, callback: Callable | None = None
) -> typer.Argument:
    return identifier_argument(
        sf_object, example, click_type=IdentifierStagePathType(), callback=callback
    )


def execution_identifier_argument(sf_object: str, example: str) -> typer.Argument:
    return typer.Argument(
        ...,
        help=f"Execution identifier of the {sf_object}. For example: {example}",
        show_default=False,
    )


def project_definition_option(is_optional: bool):
    def project_path_callback(project_path: str) -> str:
        ctx_mgr = get_cli_context_manager()
        ctx_mgr.project_path_arg = project_path
        ctx_mgr.project_is_optional = is_optional
        return project_path

    return typer.Option(
        None,
        "-p",
        "--project",
        help=f"Path where the Snowflake project is stored. "
        f"Defaults to the current working directory.",
        callback=project_path_callback,
        show_default=False,
    )


def project_env_overrides_option():
    def project_env_overrides_callback(
        env_overrides_args_list: list[str],
    ) -> dict[str, str]:
        env_overrides_args_map = {
            v.key: v.value for v in parse_key_value_variables(env_overrides_args_list)
        }
        get_cli_context_manager().project_env_overrides_args = env_overrides_args_map
        return env_overrides_args_map

    return typer.Option(
        [],
        "--env",
        help="String in the format key=value. Overrides variables from the env section used for templates.",
        callback=project_env_overrides_callback,
        show_default=False,
    )


def deprecated_flag_callback(msg: str):
    def _warning_callback(ctx: click.Context, param: click.Parameter, value: Any):
        if ctx.get_parameter_source(param.name) != click.core.ParameterSource.DEFAULT:  # type: ignore[attr-defined]
            cli_console.warning(message=msg)
        return value

    return _warning_callback


def deprecated_flag_callback_enum(msg: str):
    def _warning_callback(ctx: click.Context, param: click.Parameter, value: Any):
        if ctx.get_parameter_source(param.name) != click.core.ParameterSource.DEFAULT:  # type: ignore[attr-defined]
            cli_console.warning(message=msg)
        # Typer bug: enums passed through callback are turning into None,
        # unless their explicit value is returned ¯\_(ツ)_/¯
        return value.value

    return _warning_callback
