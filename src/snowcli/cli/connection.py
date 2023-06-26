from __future__ import annotations

import logging
import typer
from click import ClickException
from tomlkit.exceptions import KeyAlreadyPresent

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.output.printing import print_data
from snowcli.config import cli_config

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage connection to Snowflake",
)
log = logging.getLogger(__name__)


@app.command()
def list():
    """
    List Snowflake connections.
    """
    connections = cli_config.get_section("connections")
    print_data(
        [{"connection_name": k, "parameters": v} for k, v in connections.items()],
        columns=["connection_name", "parameters"],
    )


@app.command()
def add(
    connection: str = typer.Option(
        ...,
        prompt="Name for this connection",
        help="Snowflake connection name",
    ),
    account: str = typer.Option(
        ...,
        prompt="Snowflake account",
        help="Snowflake account name",
    ),
    username: str = typer.Option(
        ...,
        prompt="Snowflake username",
        help="Snowflake username",
    ),
    password: str = typer.Option(
        ...,
        prompt="Snowflake password",
        help="Snowflake password",
        hide_input=True,
    ),
):
    connection_entry = {
        "account": account,
        "username": username,
        "password": password,
    }
    try:
        cli_config.add_connection(name=connection, parameters=connection_entry)
    except KeyAlreadyPresent:
        raise ClickException(f"Connection {connection} already exists")

    log.info(f"Wrote new connection {connection} to {cli_config.file_path}")
