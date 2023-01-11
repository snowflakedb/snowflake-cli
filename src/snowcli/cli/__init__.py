from __future__ import annotations

import sys
from pathlib import Path

import typer
from rich import print

from .. import __about__
from ..config import AppConfig
from ..snowsql_config import SnowsqlConfig
from . import connection, function, procedure, stage, streamlit, warehouse

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})


def version_callback(value: bool):
    if value:
        typer.echo(f"SnowCLI Version: {__about__.VERSION}")
        raise typer.Exit()


@app.command()
def login(
    snowsql_config_path: Path = typer.Option(
        "~/.snowsql/config",
        "--config",
        "-c",
        prompt="Path to Snowsql config",
        help="snowsql config file",
    ),
    snowsql_connection_name: str = typer.Option(
        ...,
        "--connection",
        "-C",
        prompt="Connection name (for entry in snowsql config)",
        help="connection name from snowsql config file",
    ),
):
    """
    Select a Snowflake connection to use with SnowCLI.
    """
    if not snowsql_config_path.expanduser().exists():
        print(f"Path to snowsql config does not exist: {snowsql_config_path}")
        raise typer.Abort()

    cfg_snowsql = SnowsqlConfig(snowsql_config_path)
    if f"connections.{snowsql_connection_name}" not in cfg_snowsql.config:
        print(
            "Connection not found in "
            f"{snowsql_config_path}: {snowsql_connection_name}. ",
            "You can add with `snow connection add`.",
        )
        raise typer.Abort()

    cfg = AppConfig()
    cfg.config["snowsql_config_path"] = str(snowsql_config_path.expanduser())
    cfg.config["snowsql_connection_name"] = snowsql_connection_name
    cfg.save()
    print(
        "Using connection name " f"{snowsql_connection_name} in {snowsql_config_path}",
    )
    print(f"Wrote {cfg.path}")


@app.command()
def configure(
    environment: str = typer.Option(
        "dev",
        "-e",
        "--environment",
        help="Name of environment (e.g. dev, prod, staging)",
    ),
    database: str = typer.Option(
        ...,
        "--database",
        prompt="Snowflake database",
        help="Snowflake database",
    ),
    schema: str = typer.Option(
        ...,
        "--schema",
        prompt="Snowflake schema",
        help="Snowflake schema",
    ),
    role: str = typer.Option(
        ...,
        "--role",
        prompt="Snowflake role",
        help="Snowflake role",
    ),
    warehouse: str = typer.Option(
        ...,
        "--warehouse",
        prompt="Snowflake warehouse",
        help="Snowflake warehouse",
    ),
):
    """
    Configure an environment to use with your Snowflake connection.
    """
    print(f"Configuring environment #{environment}...")
    cfg = AppConfig()
    if environment in cfg.config and not typer.confirm(
        "Environment {environment} already exists. Overwrite?",
    ):
        print("Cancelling...")
        raise typer.Abort()

    cfg.config[environment] = {}
    cfg.config[environment]["database"] = database
    cfg.config[environment]["schema"] = schema
    cfg.config[environment]["role"] = role
    cfg.config[environment]["warehouse"] = warehouse
    cfg.save()
    print(f"Wrote environment {environment} to {cfg.path}")


@app.callback()
def default(
    version: bool = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
    ),
) -> None:
    """
    SnowCLI - A CLI for Snowflake
    """


app.add_typer(function.app, name="function")
app.add_typer(procedure.app, name="procedure")
app.add_typer(streamlit.app, name="streamlit")
app.add_typer(connection.app, name="connection")
app.add_typer(warehouse.app, name="warehouse")
app.add_typer(stage.app, name="stage")

if __name__ == "__main__":
    app()

if getattr(sys, "frozen", False):
    app(sys.argv[1:])
