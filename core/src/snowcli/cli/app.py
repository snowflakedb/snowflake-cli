from __future__ import annotations

import importlib
import logging
from pathlib import Path

import click
import typer

from snowcli import __about__
from snowcli.cli import loggers
from snowcli.cli.common.snow_cli_global_context import (
    SnowCliGlobalContext,
    snow_cli_global_context_manager,
    global_context_copy,
)
from snowcli.cli.main.snow_cli_main_typer import SnowCliMainTyper
from snowcli.config import config_init, cli_config
from snowcli.docs.generator import generate_docs
from snowcli.output.formats import OutputFormat
from snowcli.output.printing import print_data
from snowcli.plugin.load_plugins import PluginLoadingMode
from snowcli.plugin.plugin_registration import load_and_register_plugins_in_typer
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
        print_data(
            [
                {"key": "version", "value": __about__.VERSION},
                {"key": "default_config_file_path", "value": cli_config.file_path},
            ]
        )
        raise typer.Exit()


def _debug_callback(debug: bool):
    def modifications(context: SnowCliGlobalContext) -> SnowCliGlobalContext:
        context.debug = debug
        return context

    snow_cli_global_context_manager.update_global_context(modifications)


def _load_all_plugins_mode_callback(load_all_plugins_mode: bool):
    def modifications(context: SnowCliGlobalContext) -> SnowCliGlobalContext:
        context.plugin_loading_mode = (
            PluginLoadingMode.ALL_INSTALLED_PLUGINS
            if load_all_plugins_mode
            else PluginLoadingMode.ONLY_ENABLED_PLUGINS
        )
        return context

    snow_cli_global_context_manager.update_global_context(modifications)


def _load_and_register_plugins(enable_plugins: bool) -> None:
    if enable_plugins:
        plugin_loading_mode = global_context_copy().plugin_loading_mode
        load_and_register_plugins_in_typer(plugin_loading_mode)


@app.callback()
def default(
    debug: bool = typer.Option(
        None,
        "--debug",
        help="Print logs from level debug and higher, logs contains additional information",
        is_eager=True,
        callback=_debug_callback,
    ),
    load_all_plugins: bool = typer.Option(
        False,
        "--load-all-plugins",
        help="Load all installed plugins (even these which are explicitly disabled)",
        is_eager=True,
        callback=_load_all_plugins_mode_callback,
    ),
    configuration_file: Path = typer.Option(
        None,
        "--config-file",
        help="Specifies snowcli configuration file that should be used",
        exists=True,
        dir_okay=False,
        is_eager=True,
        callback=config_init,
    ),
    # This hidden option is required to register plugins before other options and commands evaluation.
    # It has to be placed under "safe", "load_all_plugins"
    # and "configuration_file" options to have access to their values.
    enable_plugins: bool = typer.Option(
        True,
        "--enable-plugins",
        help="Enable plugins",
        hidden=True,
        is_eager=True,
        callback=_load_and_register_plugins,
    ),
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
    output_format: OutputFormat = typer.Option(
        OutputFormat.TABLE.value,
        "--format",
        help="Specifies output format",
        case_sensitive=False,
        is_eager=True,
    ),
    verbose: bool = typer.Option(
        None,
        "--verbose",
        "-v",
        help="Print logs from level info and higher",
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
    loggers.create_loggers(verbose, debug)


def _add_typer_from_path(path: str):
    sub_module = importlib.import_module(path)
    app.add_typer(getattr(sub_module, "app"))


def _register_internal_cli_typers() -> None:
    known_sub_commands = [
        "snowcli.cli.snowpark",
        "snowcli.cli.render",
        "snowcli.cli.streamlit",
        "snowcli.cli.warehouse",
        "snowcli.cli.stage.commands",
        "snowcli.cli.plugin_management",
    ]
    for cmd in known_sub_commands:
        _add_typer_from_path(cmd)

    from snowcli.cli import sql

    app.command("sql")(sql.execute_sql)


_register_internal_cli_typers()
