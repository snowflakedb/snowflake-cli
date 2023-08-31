from __future__ import annotations

import importlib
import logging
from pathlib import Path

import typer
import click

from snowcli import __about__
from snowcli.cli.main.snow_cli_main_typer import SnowCliMainTyper
from snowcli.config import config_init, cli_config
from snowcli.docs.generator import generate_docs
from snowcli.output.formats import OutputFormat
from snowcli.output.printing import OutputData
from snowcli.pycharm_remote_debug import setup_pycharm_remote_debugger_if_provided

app: SnowCliMainTyper = SnowCliMainTyper()
log = logging.getLogger(__name__)


def _docs_callback(value: bool):
    if value:
        ctx = click.get_current_context()
        generate_docs(Path("gen_docs"), ctx.command)
        raise typer.Exit()


def _version_callback(value: bool):
    if value:
        typer.echo(f"SnowCLI Version: {__about__.VERSION}")
        raise typer.Exit()


def _info_callback(value: bool):
    if value:
        OutputData.from_list(
            [
                {"key": "version", "value": __about__.VERSION},
                {"key": "default_config_file_path", "value": cli_config.file_path},
            ],
            format_=OutputFormat.JSON,
        ).print()
        raise typer.Exit()


@app.callback()
def default(
    version: bool = typer.Option(
        None,
        "--version",
        help="Shows version of the snowcli",
        callback=_version_callback,
        is_eager=True,
    ),
    docs: bool = typer.Option(
        None,
        "--docs",
        hidden=True,
        help="Generates Snowflake CLI documentation",
        callback=_docs_callback,
        is_eager=True,
    ),
    info: bool = typer.Option(
        None,
        "--info",
        help="Shows information about the snowcli",
        callback=_info_callback,
    ),
    configuration_file: Path = typer.Option(
        None,
        "--config-file",
        help="Specifies snowcli configuration file that should be used",
        exists=True,
        dir_okay=False,
        is_eager=True,
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


def _add_typer_from_path(path: str):
    sub_module = importlib.import_module(path)
    app.add_typer(getattr(sub_module, "app"))


def _register_cli_typers() -> None:
    known_sub_commands = [
        "snowcli.cli.snowpark",
        "snowcli.cli.connection",
        "snowcli.cli.render",
        "snowcli.cli.streamlit.commands",
        "snowcli.cli.warehouse",
        "snowcli.cli.stage.commands",
        "snowcli.cli.nativeapp.commands",
    ]
    for cmd in known_sub_commands:
        _add_typer_from_path(cmd)

    from snowcli.cli import sql

    app.command("sql")(sql.execute_sql)


_register_cli_typers()
