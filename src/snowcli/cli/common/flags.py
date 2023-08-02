from __future__ import annotations

import typer


from snowcli.cli.common.snow_cli_global_context import ConnectionDetails

DEFAULT_CONTEXT_SETTINGS = {"help_option_names": ["--help", "-h"]}


ConnectionOption = typer.Option(
    None,
    "-c",
    "--connection",
    "--environment",
    help=f"Connection / environment name. If not provided then default connection will be used.",
    callback=ConnectionDetails.update_callback("connection_name"),
)

AccountOption = typer.Option(
    None,
    "--accountname",
    "--account",
    help="Name assigned to your Snowflake account.",
    callback=ConnectionDetails.update_callback("account"),
)

UserOption = typer.Option(
    None,
    "--username",
    "--user",
    help="Username to connect to Snowflake.",
    callback=ConnectionDetails.update_callback("user"),
)

PasswordOption = typer.Option(
    None,
    "--password",
    help="Snowflake password.",
    hide_input=True,
    callback=ConnectionDetails.update_callback("password"),
)

DatabaseOption = typer.Option(
    None,
    "--dbname",
    "--database",
    help="Database to use.",
    callback=ConnectionDetails.update_callback("database"),
)

SchemaOption = typer.Option(
    None,
    "--schemaname",
    "--schema",
    help=" Schema in the database to use.",
    callback=ConnectionDetails.update_callback("schema"),
)


RoleOption = typer.Option(
    None,
    "--rolename",
    "--role",
    help="Role to be used.",
    callback=ConnectionDetails.update_callback("role"),
)

WarehouseOption = typer.Option(
    None,
    "--warehouse",
    help="Warehouse to use.",
    callback=ConnectionDetails.update_callback("warehouse"),
)
