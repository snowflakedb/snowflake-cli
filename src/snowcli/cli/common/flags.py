from __future__ import annotations

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
    "-c",
    "--connection",
    "--environment",
    help=f"Connection / environment name. If not provided then default connection will be used.",
    callback=ConnectionDetails.update_callback("connection_name"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

AccountOption = typer.Option(
    None,
    "--account",
    "--accountname",
    help="Name assigned to your Snowflake account. Overrides value from connection.",
    callback=ConnectionDetails.update_callback("account"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

UserOption = typer.Option(
    None,
    "-user",
    "--username",
    help="Username to connect to Snowflake. Overrides value from connection.",
    callback=ConnectionDetails.update_callback("user"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

PasswordOption = typer.Option(
    None,
    "-p",
    "--password",
    help="Snowflake password. Overrides value from connection.",
    hide_input=True,
    callback=ConnectionDetails.update_callback("password"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

DatabaseOption = typer.Option(
    None,
    "--database",
    "--dbname",
    help="Database to use. Overrides value from connection.",
    callback=ConnectionDetails.update_callback("database"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

SchemaOption = typer.Option(
    None,
    "--schema",
    "--schemaname",
    help=" Schema in the database to use. Overrides value from connection.",
    callback=ConnectionDetails.update_callback("schema"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

RoleOption = typer.Option(
    None,
    "--role",
    "--rolename",
    help="Role to be used. Overrides value from connection.",
    callback=ConnectionDetails.update_callback("role"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

WarehouseOption = typer.Option(
    None,
    "--warehouse",
    help="Warehouse to use. Overrides value from connection.",
    callback=ConnectionDetails.update_callback("warehouse"),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

OutputFormatOption = typer.Option(
    OutputFormat.TABLE.value,
    "--format",
    help="Specifies output format",
    case_sensitive=False,
    callback=update_callback("output_format"),
    rich_help_panel=_CLI_BEHAVIOUR,
)

VerboseOption = typer.Option(
    None,
    "--verbose",
    "-v",
    help="Print logs from level info and higher",
    callback=update_callback("verbose"),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)

DebugOption = typer.Option(
    None,
    "--debug",
    help="Print logs from level debug and higher, logs contains additional information",
    callback=update_callback("enable_tracebacks"),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)
