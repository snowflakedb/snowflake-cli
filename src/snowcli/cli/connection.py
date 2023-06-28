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
    ),
    account: str = typer.Option(
        None,
        "-a",
        "--accountname",
        "--account",
        prompt="Snowflake account",
        help="Name assigned to your Snowflake account.",
    ),
    user: str = typer.Option(
        None,
        "-u",
        "--username",
        "--user",
        prompt="Snowflake username",
        help="Username to connect to Snowflake.",
    ),
    password: str = typer.Option(
        None,
        "-p",
        "--password",
        prompt="Snowflake password",
        help="Snowflake password",
        hide_input=True,
    ),
):
    """Add connection to configuration file."""
    connection_entry = {
        "account": account,
        "user": user,
        "password": password,
    }
    try:
        cli_config.add_connection(name=connection, parameters=connection_entry)
    except KeyAlreadyPresent:
        raise ClickException(f"Connection {connection} already exists")

    log.info(f"Wrote new connection {connection} to {cli_config.file_path}")
