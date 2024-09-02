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

import tempfile
from pathlib import Path
from typing import Any, Callable, Optional

import click
import typer
from click import ClickException
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.commands.common import OnErrorType
from snowflake.cli.api.commands.overrideable_parameter import OverrideableOption
from snowflake.cli.api.commands.typer_pre_execute import register_pre_execute_command
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.config import get_all_connections
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import MissingConfiguration
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.rendering.jinja import CONTEXT_KEY

DEFAULT_CONTEXT_SETTINGS = {"help_option_names": ["--help", "-h"]}

_CONNECTION_SECTION = "Connection configuration"
_CLI_BEHAVIOUR = "Global configuration"


def _callback(provide_setter: Callable[[], Callable[[Any], Any]]):
    def callback(value):
        set_value = provide_setter()
        set_value(value)
        return value

    return callback


ConnectionOption = typer.Option(
    None,
    "--connection",
    "-c",
    "--environment",
    help=f"Name of the connection, as defined in your `config.toml`. Default: `default`.",
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_connection_name
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    shell_complete=lambda _, __, ___: list(get_all_connections()),
)

TemporaryConnectionOption = typer.Option(
    False,
    "--temporary-connection",
    "-x",
    help="Uses connection defined with command line parameters, instead of one defined in config",
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_temporary_connection
    ),
    is_flag=True,
    rich_help_panel=_CONNECTION_SECTION,
)

AccountOption = typer.Option(
    None,
    "--account",
    "--accountname",
    help="Name assigned to your Snowflake account. Overrides the value specified for the connection.",
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_account
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

UserOption = typer.Option(
    None,
    "--user",
    "--username",
    help="Username to connect to Snowflake. Overrides the value specified for the connection.",
    callback=_callback(lambda: get_cli_context_manager().connection_context.set_user),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)


PLAIN_PASSWORD_MSG = "WARNING! Using --password via the CLI is insecure. Use environment variables instead."


def _password_callback(value: str):
    if value:
        cli_console.message(PLAIN_PASSWORD_MSG)

    return _callback(lambda: get_cli_context_manager().connection_context.set_password)(
        value
    )


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
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_authenticator
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

PrivateKeyPathOption = typer.Option(
    None,
    "--private-key-file",
    "--private-key-path",
    help="Snowflake private key file path. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_private_key_file
    ),
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
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_session_token
    ),
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
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_master_token
    ),
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
    help="Path to file with an OAuth token that should be used when connecting to Snowflake",
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_token_file_path
    ),
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
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_database
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

SchemaOption = typer.Option(
    None,
    "--schema",
    "--schemaname",
    help="Database schema to use. Overrides the value specified for the connection.",
    callback=_callback(lambda: get_cli_context_manager().connection_context.set_schema),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

RoleOption = typer.Option(
    None,
    "--role",
    "--rolename",
    help="Role to use. Overrides the value specified for the connection.",
    callback=_callback(lambda: get_cli_context_manager().connection_context.set_role),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

WarehouseOption = typer.Option(
    None,
    "--warehouse",
    help="Warehouse to use. Overrides the value specified for the connection.",
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_warehouse
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

MfaPasscodeOption = typer.Option(
    None,
    "--mfa-passcode",
    help="Token to use for multi-factor authentication (MFA)",
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_mfa_passcode
    ),
    prompt="MFA passcode",
    prompt_required=False,
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

EnableDiagOption = typer.Option(
    False,
    "--enable-diag",
    help="Run python connector diagnostic test",
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_enable_diag
    ),
    show_default=False,
    is_flag=True,
    rich_help_panel=_CONNECTION_SECTION,
)

# Set default via callback to avoid including tempdir path in generated docs (snow --docs).
# Use constant instead of None, as None is removed from telemetry data.
_DIAG_LOG_DEFAULT_VALUE = "<temporary_directory>"


def _diag_log_path_callback(path: str):
    if path != _DIAG_LOG_DEFAULT_VALUE:
        return path
    return tempfile.gettempdir()


DiagLogPathOption: Path = typer.Option(
    _DIAG_LOG_DEFAULT_VALUE,
    "--diag-log-path",
    help="Diagnostic report path",
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_diag_log_path
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    writable=True,
)

DiagAllowlistPathOption: Path = typer.Option(
    None,
    "--diag-allowlist-path",
    help="Diagnostic report path to optional allowlist",
    callback=_callback(
        lambda: get_cli_context_manager().connection_context.set_diag_allowlist_path
    ),
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
    callback=_callback(lambda: get_cli_context_manager().set_output_format),
    rich_help_panel=_CLI_BEHAVIOUR,
)

SilentOption = typer.Option(
    False,
    "--silent",
    help="Turns off intermediate output to console.",
    callback=_callback(lambda: get_cli_context_manager().set_silent),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
    is_eager=True,
)

VerboseOption = typer.Option(
    False,
    "--verbose",
    "-v",
    help="Displays log entries for log levels `info` and higher.",
    callback=_callback(lambda: get_cli_context_manager().set_verbose),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)

DebugOption = typer.Option(
    False,
    "--debug",
    help="Displays log entries for log levels `debug` and higher; debug logs contains additional information.",
    callback=_callback(lambda: get_cli_context_manager().set_enable_tracebacks),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
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

OnErrorOption = typer.Option(
    OnErrorType.BREAK.value,
    "--on-error",
    help="What to do when an error occurs. Defaults to break.",
)

NoInteractiveOption = typer.Option(False, "--no-interactive", help="Disable prompting.")


def entity_argument(entity_type: str) -> typer.Argument:
    return typer.Argument(None, help=f"ID of {entity_type} entity.")


def variables_option(description: str):
    return typer.Option(
        None,
        "--variable",
        "-D",
        help=description,
        show_default=False,
    )


ExecuteVariablesOption = variables_option(
    'Variables for the execution context. For example: `-D "<key>=<value>"`. '
    "For SQL files variables are use to expand the template and any unknown variable will cause an error. "
    "For Python files variables are used to update os.environ dictionary. Provided keys are capitalized to adhere to best practices."
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
        callback=_callback(lambda: get_cli_context_manager().set_experimental),
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


def identifier_argument(
    sf_object: str,
    example: str,
    click_type: click.ParamType = IdentifierType(),
    callback: Callable | None = None,
) -> typer.Argument:
    return typer.Argument(
        ...,
        help=f"Identifier of the {sf_object}. For example: {example}",
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


def execution_identifier_argument(sf_object: str, example: str) -> typer.Argument:
    return typer.Argument(
        ...,
        help=f"Execution identifier of the {sf_object}. For example: {example}",
        show_default=False,
    )


def register_project_definition(is_optional: bool) -> None:
    cli_context_manager = get_cli_context_manager()
    project_path = cli_context_manager.project_path_arg
    env_overrides_args = cli_context_manager.project_env_overrides_args

    dm = DefinitionManager(project_path, {CONTEXT_KEY: {"env": env_overrides_args}})
    project_definition = dm.project_definition
    project_root = dm.project_root
    template_context = dm.template_context

    if not dm.has_definition_file and not is_optional:
        raise MissingConfiguration(
            "Cannot find project definition (snowflake.yml). Please provide a path to the project or run this command in a valid project directory."
        )

    cli_context_manager.set_project_definition(project_definition)
    cli_context_manager.set_project_root(project_root)
    cli_context_manager.set_template_context(template_context)


def project_definition_option(is_optional: bool):
    def project_definition_callback(project_path: str) -> None:
        get_cli_context_manager().set_project_path_arg(project_path)
        register_pre_execute_command(lambda: register_project_definition(is_optional))

    return typer.Option(
        None,
        "-p",
        "--project",
        help=f"Path where Snowflake project resides. "
        f"Defaults to current working directory.",
        callback=_callback(lambda: project_definition_callback),
        show_default=False,
    )


def project_env_overrides_option():
    def project_env_overrides_callback(env_overrides_args_list: list[str]) -> None:
        env_overrides_args_map = {
            v.key: v.value for v in parse_key_value_variables(env_overrides_args_list)
        }
        get_cli_context_manager().set_project_env_overrides_args(env_overrides_args_map)

    return typer.Option(
        [],
        "--env",
        help="String in format of key=value. Overrides variables from env section used for templates.",
        callback=_callback(lambda: project_env_overrides_callback),
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
