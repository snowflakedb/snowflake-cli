from __future__ import annotations

from typing import Optional

import typer

from snowcli.cli.common.snow_cli_global_context import (
    ConnectionDetails,
    update_callback,
)
from snowcli.output.formats import OutputFormat

DEFAULT_CONTEXT_SETTINGS = {"help_option_names": ["--help", "-h"]}


_CONNECTION_SECTION = "Connection configuration"
_CLI_BEHAVIOUR = "Global configuration"


ConnectionOption = typer.Option(
    None,
    "--connection",
    "-c",
    "--environment",
    help=f"Name of the connection, as defined in your `config.toml`. Default: `dev`.",
    callback=ConnectionDetails.update_callback("connection_name"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

AccountOption = typer.Option(
    None,
    "--account",
    "--accountname",
    help="Name assigned to your Snowflake account. Overrides the value specified for the connection.",
    callback=ConnectionDetails.update_callback("account"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

UserOption = typer.Option(
    None,
    "--user",
    "--username",
    help="Username to connect to Snowflake. Overrides the value specified for the connection.",
    callback=ConnectionDetails.update_callback("user"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

PasswordOption = typer.Option(
    None,
    "--password",
    help="Snowflake password. Overrides the value specified for the connection.",
    hide_input=True,
    callback=ConnectionDetails.update_callback("password"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

AuthenticatorOption = typer.Option(
    None,
    "--authenticator",
    help="Snowflake authenticator. Overrides the value specified for the connection.",
    hide_input=True,
    callback=ConnectionDetails.update_callback("authenticator"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

PrivateKeyPathOption = typer.Option(
    None,
    "--private-key-path",
    help="Snowflake private key path. Overrides the value specified for the connection.",
    hide_input=True,
    callback=ConnectionDetails.update_callback("private_key_path"),
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
    callback=ConnectionDetails.update_callback("database"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

SchemaOption = typer.Option(
    None,
    "--schema",
    "--schemaname",
    help="Database schema to use. Overrides the value specified for the connection.",
    callback=ConnectionDetails.update_callback("schema"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

RoleOption = typer.Option(
    None,
    "--role",
    "--rolename",
    help="Role to use. Overrides the value specified for the connection.",
    callback=ConnectionDetails.update_callback("role"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

WarehouseOption = typer.Option(
    None,
    "--warehouse",
    help="Warehouse to use. Overrides the value specified for the connection.",
    callback=ConnectionDetails.update_callback("warehouse"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

OutputFormatOption = typer.Option(
    OutputFormat.TABLE.value,
    "--format",
    help="Specifies the output format.",
    case_sensitive=False,
    callback=update_callback("output_format"),
    rich_help_panel=_CLI_BEHAVIOUR,
)

VerboseOption = typer.Option(
    None,
    "--verbose",
    "-v",
    help="Displays log entries for log levels `info` and higher.",
    callback=update_callback("verbose"),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)

DebugOption = typer.Option(
    None,
    "--debug",
    help="Displays log entries for log levels `debug` and higher; debug logs contains additional information.",
    callback=update_callback("enable_tracebacks"),
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
        callback=update_callback("experimental"),
        is_flag=True,
        rich_help_panel=_CLI_BEHAVIOUR,
    )
