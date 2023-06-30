from __future__ import annotations

import logging

import typer
from click import ClickException
from click.types import StringParamType
from tomlkit.exceptions import KeyAlreadyPresent

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.output.printing import print_data
from snowcli.config import cli_config

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage connection to Snowflake",
)
log = logging.getLogger(__name__)


class EmptyInput:
    def __repr__(self):
        return "optional"


class OptionalPrompt(StringParamType):
    def convert(self, value, param, ctx):
        return None if isinstance(value, EmptyInput) else value


@app.command(name="list")
def list_connections():
    """
    List configured connections.
    """
    connections = cli_config.get_section("connections")
    print_data(
        [{"connection_name": k, "parameters": v} for k, v in connections.items()],
        columns=["connection_name", "parameters"],
    )


@app.command()
def add(
    connection: str = typer.Option(
        None,
        "-n",
        "--connection-name",
        prompt="Name for this connection",
        help="Name of the new connection",
        show_default=False,
    ),
    account: str = typer.Option(
        None,
        "-a",
        "--accountname",
        "--account",
        prompt="Snowflake account name",
        help="Account name to be used to authenticate with Snowflake.",
        show_default=False,
    ),
    user: str = typer.Option(
        None,
        "-u",
        "--username",
        "--user",
        prompt="Snowflake username",
        show_default=False,
        help="Username to connect to Snowflake.",
    ),
    password: str = typer.Option(
        EmptyInput(),
        "-p",
        "--password",
        click_type=OptionalPrompt(),
        prompt="Snowflake password",
        help="Snowflake password",
        hide_input=True,
    ),
    role: str = typer.Option(
        EmptyInput(),
        "-r",
        "--role",
        click_type=OptionalPrompt(),
        prompt="Role for the connection",
        help="Role to use on Snowflake.",
    ),
    warehouse: str = typer.Option(
        EmptyInput(),
        "-w",
        "--warehouse",
        click_type=OptionalPrompt(),
        prompt="Warehouse for the connection",
        help="Warehouse to use on Snowflake.",
    ),
    database: str = typer.Option(
        EmptyInput(),
        "-d",
        "--database",
        click_type=OptionalPrompt(),
        prompt="Database for the connection",
        help="Database to use on Snowflake.",
    ),
    schema: str = typer.Option(
        EmptyInput(),
        "-s",
        "--schema",
        click_type=OptionalPrompt(),
        prompt="Schema for the connection",
        help="Schema to use on Snowflake.",
    ),
    host: str = typer.Option(
        EmptyInput(),
        "-h",
        "--host",
        click_type=OptionalPrompt(),
        prompt="Connection host",
        help="The host name the connection attempts to connect to.",
    ),
    port: int = typer.Option(
        EmptyInput(),
        "-P",
        "--port",
        click_type=OptionalPrompt(),
        prompt="Connection port",
        help="The port to communicate with on the host.",
    ),
    region: str = typer.Option(
        EmptyInput(),
        "-R",
        "--region",
        click_type=OptionalPrompt(),
        prompt="Snowflake region",
        help="Region name if not the default Snowflake deployment.",
    ),
):
    """Add connection to configuration file."""
    connection_entry = {
        "account": account,
        "user": user,
        "password": password,
        "host": host,
        "region": region,
        "port": port,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
        "role": role,
    }
    connection_entry = {k: v for k, v in connection_entry.items() if v is not None}

    try:
        cli_config.add_connection(name=connection, parameters=connection_entry)
    except KeyAlreadyPresent:
        raise ClickException(f"Connection {connection} already exists")

    log.info(f"Wrote new connection {connection} to {cli_config.file_path}")
