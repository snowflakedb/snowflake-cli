from __future__ import annotations

import importlib
import logging
from pathlib import Path

import typer

from snowcli import __about__
from snowcli.cli import loggers
from snowcli.cli.common.snow_cli_global_context import (
    SnowCliGlobalContext,
    snow_cli_global_context_manager,
)
from snowcli.cli.main.snow_cli_main_typer import SnowCliMainTyper
from snowcli.config import config_init, cli_config
from snowcli.output.formats import OutputFormat
from snowcli.output.printing import print_data
from snowcli.pycharm_remote_debug import setup_pycharm_remote_debugger_if_provided

app: SnowCliMainTyper = SnowCliMainTyper()
log = logging.getLogger(__name__)


def _version_callback(value: bool):
    if value:
        typer.echo(f"SnowCLI Version: {__about__.VERSION}")
        raise typer.Exit()


def _info_callback(value: bool):
    if value:
        print_data(
            [
                {"key": "version", "value": __about__.VERSION},
                {"key": "default_config_file_path", "value": cli_config.file_path},
            ]
        )
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
        help="Shows version of the snowcli",
        callback=_version_callback,
        is_eager=True,
    ),
    info: bool = typer.Option(
        None,
        "--info",
        help="Shows information about the snowcli",
        callback=_info_callback,
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


def _add_typer_from_path(path: str):
    sub_module = importlib.import_module(path)
    app.add_typer(getattr(sub_module, "app"))


def _register_cli_typers() -> None:
    known_sub_commands = [
        "snowcli.cli.snowpark",
        "snowcli.cli.connection",
        "snowcli.cli.render",
        "snowcli.cli.streamlit",
        "snowcli.cli.warehouse",
        "snowcli.cli.stage",
    ]
    for cmd in known_sub_commands:
        _add_typer_from_path(cmd)

    from snowcli.cli import sql

    app.command("sql")(sql.execute_sql)


_register_cli_typers()
