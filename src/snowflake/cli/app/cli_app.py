from __future__ import annotations

import logging
import platform
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import click
import typer
from click import Context
from snowflake.cli import __about__
from snowflake.cli.api import Api, api_provider
from snowflake.cli.api.config import config_init
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import CollectionResult
from snowflake.cli.app.api_impl.plugin.plugin_config_provider_impl import (
    PluginConfigProviderImpl,
)
from snowflake.cli.app.commands_registration.commands_registration_with_callbacks import (
    CommandsRegistrationWithCallbacks,
)
from snowflake.cli.app.dev.commands_structure import generate_commands_structure
from snowflake.cli.app.dev.docs.generator import generate_docs
from snowflake.cli.app.dev.pycharm_remote_debug import (
    setup_pycharm_remote_debugger_if_provided,
)
from snowflake.cli.app.main_typer import SnowCliMainTyper
from snowflake.cli.app.printing import print_result
from snowflake.connector.config_manager import CONFIG_MANAGER

app: SnowCliMainTyper = SnowCliMainTyper()
log = logging.getLogger(__name__)

_api = Api(plugin_config_provider=PluginConfigProviderImpl())
api_provider.register_api(_api)

_commands_registration = CommandsRegistrationWithCallbacks(_api.plugin_config_provider)


@dataclass
class AppContextHolder:
    # needed to access the context from tests
    app_context: Optional[Context] = None


app_context_holder = AppContextHolder()


def _exit_with_cleanup():
    _commands_registration.reset_running_instance_registration_state()
    raise typer.Exit()


def _do_not_execute_on_completion(callback):
    def enriched_callback(value):
        if click.get_current_context().resilient_parsing:
            return
        callback(value)

    return enriched_callback


def _commands_registration_callback(value: bool):
    if value:
        _commands_registration.register_commands_if_ready_and_not_registered_yet()
    # required to make the tests working
    # because a single test can execute multiple commands using always the same "app" instance
    _commands_registration.reset_running_instance_registration_state()
    app_context_holder.app_context = click.get_current_context()


@_commands_registration.before
def _config_init_callback(configuration_file: Optional[Path]):
    config_init(configuration_file)


@_commands_registration.before
def _disable_external_command_plugins_callback(value: bool):
    if value:
        _commands_registration.disable_external_command_plugins()


@_do_not_execute_on_completion
@_commands_registration.after
def _docs_callback(value: bool):
    if value:
        ctx = click.get_current_context()
        generate_docs(Path("gen_docs"), ctx.command)
        _exit_with_cleanup()


@_do_not_execute_on_completion
@_commands_registration.after
def _commands_structure_callback(value: bool):
    if value:
        ctx = click.get_current_context()
        generate_commands_structure(ctx.command).print_node()
        _exit_with_cleanup()


@_do_not_execute_on_completion
def _version_callback(value: bool):
    if value:
        typer.echo(f"SnowCLI Version: {__about__.VERSION}")
        _exit_with_cleanup()


@_do_not_execute_on_completion
def _info_callback(value: bool):
    if value:
        result = CollectionResult(
            [
                {"key": "version", "value": __about__.VERSION},
                {
                    "key": "default_config_file_path",
                    "value": str(CONFIG_MANAGER.file_path),
                },
                {"key": "python_version", "value": sys.version},
                {"key": "system_info", "value": platform.platform()},
            ],
        )
        print_result(result, output_format=OutputFormat.JSON)
        _exit_with_cleanup()


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
    structure: bool = typer.Option(
        None,
        "--structure",
        hidden=True,
        help="Prints Snowflake CLI structure of commands",
        callback=_commands_structure_callback,
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
        callback=_config_init_callback,
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
    disable_external_command_plugins: bool = typer.Option(
        None,
        "--disable-external-command-plugins",
        help="Disable external command plugins",
        callback=_disable_external_command_plugins_callback,
        is_eager=True,
    ),
    # THIS OPTION SHOULD BE THE LAST OPTION IN THE LIST!
    # ---
    # This is a hidden artificial option used only to guarantee execution of commands registration
    # and make this guaranty not dependent on other callbacks.
    # Commands registration is invoked as soon as all callbacks
    # decorated with "_commands_registration.before" are executed
    # but if there are no such callbacks (at the result of possible future changes)
    # then we need to invoke commands registration manually.
    #
    # This option is also responsible for resetting registration state for test purposes.
    commands_registration: bool = typer.Option(
        True,
        "--commands-registration",
        help="Commands registration",
        hidden=True,
        is_eager=True,
        callback=_commands_registration_callback,
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
