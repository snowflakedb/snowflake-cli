from __future__ import annotations

import logging
import pkgutil
import sys
import typer
from collections.abc import Container
from pathlib import Path

from snowcli import __about__
from snowcli.cli import (
    connection,
    render,
    sql,
    streamlit,
)
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.common.snow_cli_global_context import (
    snow_cli_global_context_manager,
    SnowCliGlobalContext,
)
from snowcli.cli.main.snow_cli_main_typer import SnowCliMainTyper
from snowcli.cli import loggers
from snowcli.cli.snowpark import app as snowpark_app
from snowcli.config import AppConfig
from snowcli.output.formats import OutputFormat
from snowcli.connection_config import ConnectionConfigs

app = SnowCliMainTyper()
log = logging.getLogger(__name__)


def _version_callback(value: bool):
    if value:
        typer.echo(f"SnowCLI Version: {__about__.VERSION}")
        raise typer.Exit()


def setup_global_context(debug: bool):
    def modifications(context: SnowCliGlobalContext) -> SnowCliGlobalContext:
        context.enable_tracebacks = debug
        return context

    snow_cli_global_context_manager.update_global_context(modifications)


@app.command()
def login(
    snowsql_config_path: Path = typer.Option(
        "~/.snowsql/config",
        "--config",
        "-c",
        prompt="Path to Snowsql config",
        help="snowsql config file",
    ),
    connection_name: str = typer.Option(
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
        log.error(f"Path to snowsql config does not exist: {snowsql_config_path}")
        raise typer.Abort()

    connection_configs = ConnectionConfigs(snowsql_config_path)
    if not connection_configs.connection_exists(connection_name):
        log.error(
            f"Connection not found in {snowsql_config_path}: {connection_name}. "
            "You can add with `snow connection add`."
        )
        raise typer.Abort()

    cfg = AppConfig()
    cfg.config["snowsql_config_path"] = str(snowsql_config_path.expanduser())
    cfg.config["snowsql_connection_name"] = connection_name
    cfg.save()
    log.info(f"Using connection name {connection_name} in {snowsql_config_path}")
    log.info(f"Wrote {cfg.path}")


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
    log.info(f"Configuring environment {environment}...")
    cfg = AppConfig()
    if environment in cfg.config and not typer.confirm(
        "Environment {environment} already exists. Overwrite?",
    ):
        log.error("Cancelling...")
        raise typer.Abort()

    cfg.config[environment] = {}
    cfg.config[environment]["database"] = database
    cfg.config[environment]["schema"] = schema
    cfg.config[environment]["role"] = role
    cfg.config[environment]["warehouse"] = warehouse
    cfg.save()
    log.info(f"Wrote environment {environment} to {cfg.path}")


@app.callback()
def default(
    version: bool = typer.Option(
        None,
        "--version",
        help="Prints version of the snowcli",
        callback=_version_callback,
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
    verbose: bool = typer.Option(
        None,
        "--verbose",
        "-v",
        help="Print logs from level info and higher",
    ),
    debug: bool = typer.Option(
        None,
        "--debug",
        help="Print logs from level debug and higher, logs contains additional information",
    ),
) -> None:
    """
    SnowCLI - A CLI for Snowflake
    """
    loggers.create_loggers(verbose, debug)
    setup_global_context(debug=debug)


MODULE_IGNORE_SET = frozenset(("main", "procedure_coverage"))


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
