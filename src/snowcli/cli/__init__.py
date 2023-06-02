from __future__ import annotations

import pkgutil
import sys
from collections.abc import Container
from pathlib import Path

import typer
from rich import print

from snowcli import __about__
from snowcli.cli import (
    connection,
    render,
    sql,
    streamlit,
)
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark import app as snowpark_app
from snowcli.config import AppConfig
from snowcli.output.formats import OutputFormat
from snowcli.snowsql_config import SnowsqlConfig

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    pretty_exceptions_show_locals=False,
)


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
        help="Prints version of the snowcli",
        callback=version_callback,
        is_eager=True,
    ),
    output_format: OutputFormat = typer.Option(
        OutputFormat.TABLE.value,
        "--format",
        help="Specifies output format",
        case_sensitive=False,
        is_eager=True,
    ),
    configuration_file: Path = typer.Option(
        None,
        "--config-file",
        help="Specifies snowcli configuration file that should be used",
        exists=True,
        dir_okay=False,
        is_eager=True,
    ),
) -> None:
    """
    SnowCLI - A CLI for Snowflake
    """


MODULE_IGNORE_SET = frozenset(("procedure_coverage",))


def register_cli_typers(ignore_container: Container[str] = MODULE_IGNORE_SET) -> None:
    for _, name, _ in pkgutil.walk_packages(__path__):
        if name not in ignore_container:
            cli_app = __import__(f"{__name__}.{name}", fromlist=["_trash"])
            try:
                app.add_typer(cli_app.app, name=name)
            except AttributeError:
                # Ignore modules that don't define app global
                pass


register_cli_typers()

app.command("sql")(sql.execute_sql)
app.add_typer(snowpark_app)


if __name__ == "__main__":
    app()

if getattr(sys, "frozen", False):
    app(sys.argv[1:])
