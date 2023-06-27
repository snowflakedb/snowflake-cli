from __future__ import annotations

import typer
from snowcli.utils import check_for_connection

DEFAULT_CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


ConnectionOption = typer.Option(
    "dev",
    "-c",
    "--connection",
    "--environment",
    help="Connection / environment name",
    callback=check_for_connection,
    is_eager=True,
)

AccountOption = typer.Option(
    None,
    "-a",
    "--accountname",
    "--account",
    help="Name assigned to your Snowflake account.",
)

UserOption = typer.Option(
    None,
    "-u",
    "--username",
    "--user",
    help="Username to connect to Snowflake.",
)

PasswordOption = typer.Option(
    None,
    "-p",
    "--password",
    help="Snowflake password",
    hide_input=True,
)

DatabaseOption = typer.Option(
    None,
    "-d",
    "--dbname",
    "--database",
    help="Database to use.",
)

SchemaOption = typer.Option(
    None,
    "-s",
    "--schemaname",
    "--schema",
    help=" Schema in the database to use.",
)

RoleOption = typer.Option(None, "-r", "--rolename", "--role", help="Role to be used")

WarehouseOption = typer.Option(None, "-w", "--warehouse", help="Warehouse to use.")
