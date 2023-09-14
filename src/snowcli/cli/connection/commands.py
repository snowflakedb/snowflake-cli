from __future__ import annotations

import logging

import typer
from click import ClickException
from click.types import StringParamType
from tomlkit.exceptions import KeyAlreadyPresent

from snowcli.cli.common.decorators import global_options
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.config import cli_config
from snowcli.output.decorators import with_output
from snowcli.output.types import CollectionResult, CommandResult, MessageResult
from snowcli.snow_connector import connect_to_snowflake

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="connection",
    help="Manages connections to Snowflake.",
)
log = logging.getLogger(__name__)


class EmptyInput:
    def __repr__(self):
        return "optional"


class OptionalPrompt(StringParamType):
    def convert(self, value, param, ctx):
        return None if isinstance(value, EmptyInput) else value


def _mask_password(connection_params: dict):
    if "password" in connection_params:
        connection_params["password"] = "****"
    return connection_params


@app.command(name="list")
@with_output
@global_options
def list_connections(**options) -> CommandResult:
    """
    Lists configured connections.
    """
    connections = cli_config.get_section("connections")
    result = (
        {"connection_name": k, "parameters": _mask_password(v)}
        for k, v in connections.items()
    )
    return CollectionResult(result)


def require_integer(field_name: str):
    def callback(value: str):
        if value is None:
            return None
        if value.isdigit():
            return value
        raise ClickException(f"Value of {field_name} must be integer")

    return callback


@app.command()
@global_options
@with_output
def add(
    connection_name: str = typer.Option(
        None,
        "--connection-name",
        "-n",
        prompt="Name for this connection",
        help="Name of the new connection.",
        show_default=False,
    ),
    account: str = typer.Option(
        None,
        "--account",
        "-a",
        "--accountname",
        prompt="Snowflake account name",
        help="Account name to use when authenticating with Snowflake.",
        show_default=False,
    ),
    user: str = typer.Option(
        None,
        "--user",
        "-u",
        "--username",
        prompt="Snowflake username",
        show_default=False,
        help="Username to connect to Snowflake.",
    ),
    password: str = typer.Option(
        EmptyInput(),
        "--password",
        "-p",
        click_type=OptionalPrompt(),
        prompt="Snowflake password",
        help="Snowflake password.",
        hide_input=True,
    ),
    role: str = typer.Option(
        EmptyInput(),
        "--role",
        "-r",
        click_type=OptionalPrompt(),
        prompt="Role for the connection",
        help="Role to use on Snowflake.",
    ),
    warehouse: str = typer.Option(
        EmptyInput(),
        "--warehouse",
        "-w",
        click_type=OptionalPrompt(),
        prompt="Warehouse for the connection",
        help="Warehouse to use on Snowflake.",
    ),
    database: str = typer.Option(
        EmptyInput(),
        "--database",
        "-d",
        click_type=OptionalPrompt(),
        prompt="Database for the connection",
        help="Database to use on Snowflake.",
    ),
    schema: str = typer.Option(
        EmptyInput(),
        "--schema",
        "-s",
        click_type=OptionalPrompt(),
        prompt="Schema for the connection",
        help="Schema to use on Snowflake.",
    ),
    host: str = typer.Option(
        EmptyInput(),
        "--host",
        "-h",
        click_type=OptionalPrompt(),
        prompt="Connection host",
        help="Host name the connection attempts to connect to Snowflake.",
    ),
    port: int = typer.Option(
        EmptyInput(),
        "--port",
        "-P",
        click_type=OptionalPrompt(),
        prompt="Connection port",
        help="Port to communicate with on the host.",
        callback=require_integer(field_name="port"),
    ),
    region: str = typer.Option(
        EmptyInput(),
        "--region",
        "-R",
        click_type=OptionalPrompt(),
        prompt="Snowflake region",
        help="Region name if not the default Snowflake deployment.",
    ),
    **options,
) -> CommandResult:
    """Adds a connection to configuration file."""
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
        cli_config.add_connection(name=connection_name, parameters=connection_entry)
    except KeyAlreadyPresent:
        raise ClickException(f"Connection {connection_name} already exists")

    return MessageResult(
        f"Wrote new connection {connection_name} to {cli_config.file_path}"
    )


@app.command()
@global_options
@with_output
def test(connection: str = ConnectionOption, **options) -> CommandResult:
    """
    Tests the connection to Snowflake.
    """
    connect_to_snowflake(connection_name=connection)
    return MessageResult("OK")
