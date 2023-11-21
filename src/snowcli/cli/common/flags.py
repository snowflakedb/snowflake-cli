from __future__ import annotations

from typing import Optional, Callable, Any

import typer

from snowcli.cli.common.cli_global_context import cli_context_manager
from snowcli.output.formats import OutputFormat

DEFAULT_CONTEXT_SETTINGS = {"help_option_names": ["--help", "-h"]}

_CONNECTION_SECTION = "Connection configuration"
_CLI_BEHAVIOUR = "Global configuration"


def _set_value_callback(update: Callable[[Any], Any]):
    def callback(value):
        update(value)
        return value

    return callback


def _update_cli_context_callback(attr: str):
    return _set_value_callback(
        lambda value: cli_context_manager.__setattr__(attr, value)
    )


def _update_cli_connection_context_callback(attr: str):
    return _set_value_callback(
        lambda value: cli_context_manager.connection_context.__setattr__(attr, value)
    )


ConnectionOption = typer.Option(
    None,
    "--connection",
    "-c",
    "--environment",
    help=f"Name of the connection, as defined in your `config.toml`. Default: `dev`.",
    callback=_update_cli_connection_context_callback("connection_name"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

TemporaryConnectionOption = typer.Option(
    False,
    "--temporary-connection",
    "-x",
    help="Uses connection defined with command line parameters, instead of one defined in config",
    callback=_update_cli_connection_context_callback("temporary_connection"),
    is_flag=True,
    rich_help_panel=_CONNECTION_SECTION,
)

AccountOption = typer.Option(
    None,
    "--account",
    "--accountname",
    help="Name assigned to your Snowflake account. Overrides the value specified for the connection.",
    callback=_update_cli_connection_context_callback("account"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

UserOption = typer.Option(
    None,
    "--user",
    "--username",
    help="Username to connect to Snowflake. Overrides the value specified for the connection.",
    callback=_update_cli_connection_context_callback("user"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

PasswordOption = typer.Option(
    None,
    "--password",
    help="Snowflake password. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_update_cli_connection_context_callback("password"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

AuthenticatorOption = typer.Option(
    None,
    "--authenticator",
    help="Snowflake authenticator. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_update_cli_connection_context_callback("authenticator"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

PrivateKeyPathOption = typer.Option(
    None,
    "--private-key-path",
    help="Snowflake private key path. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_update_cli_connection_context_callback("private_key_path"),
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
    callback=_update_cli_connection_context_callback("database"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

SchemaOption = typer.Option(
    None,
    "--schema",
    "--schemaname",
    help="Database schema to use. Overrides the value specified for the connection.",
    callback=_update_cli_connection_context_callback("schema"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

RoleOption = typer.Option(
    None,
    "--role",
    "--rolename",
    help="Role to use. Overrides the value specified for the connection.",
    callback=_update_cli_connection_context_callback("role"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

WarehouseOption = typer.Option(
    None,
    "--warehouse",
    help="Warehouse to use. Overrides the value specified for the connection.",
    callback=_update_cli_connection_context_callback("warehouse"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

OutputFormatOption = typer.Option(
    OutputFormat.TABLE.value,
    "--format",
    help="Specifies the output format.",
    case_sensitive=False,
    callback=_update_cli_context_callback("output_format"),
    rich_help_panel=_CLI_BEHAVIOUR,
)

VerboseOption = typer.Option(
    None,
    "--verbose",
    "-v",
    help="Displays log entries for log levels `info` and higher.",
    callback=_update_cli_context_callback("verbose"),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)

DebugOption = typer.Option(
    None,
    "--debug",
    help="Displays log entries for log levels `debug` and higher; debug logs contains additional information.",
    callback=_update_cli_context_callback("enable_tracebacks"),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
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
        None,
        "--experimental",
        help=help_text,
        hidden=True,
        callback=_update_cli_context_callback("experimental"),
        is_flag=True,
        rich_help_panel=_CLI_BEHAVIOUR,
    )


def identifier_argument(object: str, example: str) -> typer.Argument:
    return typer.Argument(
        ..., help=f"Identifier of the {object}. For example: {example}"
    )


def execution_identifier_argument(object: str, example: str) -> typer.Argument:
    return typer.Argument(
        ..., help=f"Execution identifier of the {object}. For example: {example}"
    )
