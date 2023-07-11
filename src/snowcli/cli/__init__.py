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
from snowcli.cli.nativeapps import app as nativeapp_app
from snowcli.config import config_init
from snowcli.output.formats import OutputFormat
from snowcli.pycharm_remote_debug import setup_pycharm_remote_debugger_if_provided

app: SnowCliMainTyper = SnowCliMainTyper()
log = logging.getLogger(__name__)


def _version_callback(value: bool):
    if value:
        typer.echo(f"SnowCLI Version: {__about__.VERSION}")
        raise typer.Exit()


def setup_global_context(debug: bool):
    """
    Setup global state (accessible in whole CLI code) using options passed in SNOW CLI invocation.
    """

    def modifications(context: SnowCliGlobalContext) -> SnowCliGlobalContext:
        context.enable_tracebacks = debug
        return context

    snow_cli_global_context_manager.update_global_context(modifications)


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
    pycharm_debug_library_path: str = typer.Option(
        None,
        "--pycharm-debug-library-path",
        hidden=True,
    ),
    pycharm_debug_server_host: str = typer.Option(
        "localhost",
        "--pycharm-debug-server-host",
        hidden=True,
    ),
    pycharm_debug_server_port: int = typer.Option(
        12345,
        "--pycharm-debug-server-port",
        hidden=True,
    ),
) -> None:
    """
    SnowCLI - A CLI for Snowflake
    """
    setup_pycharm_remote_debugger_if_provided(
        pycharm_debug_library_path=pycharm_debug_library_path,
        pycharm_debug_server_host=pycharm_debug_server_host,
        pycharm_debug_server_port=pycharm_debug_server_port,
    )
    config_init(configuration_file)
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
app.add_typer(snowpark_app)  # type: ignore
app.add_typer(nativeapp_app)

if __name__ == "__main__":
    app()

if getattr(sys, "frozen", False):
    app(sys.argv[1:])
