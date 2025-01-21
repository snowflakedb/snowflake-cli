# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
import os
import platform
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import click
import typer
from click import Context
from snowflake.cli import __about__
from snowflake.cli._app.api_impl.plugin.plugin_config_provider_impl import (
    PluginConfigProviderImpl,
)
from snowflake.cli._app.commands_registration.commands_registration_with_callbacks import (
    CommandsRegistrationWithCallbacks,
)
from snowflake.cli._app.dev.commands_structure import generate_commands_structure
from snowflake.cli._app.dev.docs.generator import generate_docs
from snowflake.cli._app.dev.pycharm_remote_debug import (
    setup_pycharm_remote_debugger_if_provided,
)
from snowflake.cli._app.main_typer import SnowCliMainTyper
from snowflake.cli._app.printing import MessageResult, print_result
from snowflake.cli._app.version_check import (
    get_new_version_msg,
    show_new_version_banner_callback,
)
from snowflake.cli.api import Api, api_provider
from snowflake.cli.api.config import config_init
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import CollectionResult
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.config_manager import CONFIG_MANAGER

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
        generate_docs(SecurePath("gen_docs"), ctx.command)
        _exit_with_cleanup()


@_do_not_execute_on_completion
@_commands_registration.after
def _help_callback(value: bool):
    if value:
        ctx = click.get_current_context()
        typer.echo(ctx.get_help())
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
        print_result(MessageResult(f"Snowflake CLI version: {__about__.VERSION}"))
        _exit_with_cleanup()


from snowflake.cli.api.config import get_feature_flags_section


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
                {"key": "feature_flags", "value": get_feature_flags_section()},
                {"key": "SNOWFLAKE_HOME", "value": os.getenv("SNOWFLAKE_HOME")},
            ],
        )
        print_result(result, output_format=OutputFormat.JSON)
        _exit_with_cleanup()


def app_factory() -> SnowCliMainTyper:
    app = SnowCliMainTyper()
    new_version_msg = get_new_version_msg()

    @app.callback(
        invoke_without_command=True,
        epilog=new_version_msg,
        result_callback=show_new_version_banner_callback(new_version_msg),
        add_help_option=False,  # custom_help option added below
        help=f"Snowflake CLI tool for developers [v{__about__.VERSION}]",
    )
    def default(
        ctx: typer.Context,
        # We need a custom help option with _help_callback called after command registration
        # to have all commands visible in the help.
        # This is required since click 8.1.8, when the default help option
        # has started to being executed before our eager options, including command registration.
        custom_help: bool = typer.Option(
            None,
            "--help",
            "-h",
            help="Show this message and exit.",
            callback=_help_callback,
            is_eager=True,
        ),
        version: bool = typer.Option(
            None,
            "--version",
            help="Shows version of the Snowflake CLI",
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
            help="Shows information about the Snowflake CLI",
            callback=_info_callback,
        ),
        configuration_file: Path = typer.Option(
            None,
            "--config-file",
            help="Specifies Snowflake CLI configuration file that should be used",
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
            hidden=True,
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
        Snowflake CLI tool for developers.
        """
        if not ctx.invoked_subcommand:
            typer.echo(ctx.get_help())
        setup_pycharm_remote_debugger_if_provided(
            pycharm_debug_library_path=pycharm_debug_library_path,
            pycharm_debug_server_host=pycharm_debug_server_host,
            pycharm_debug_server_port=pycharm_debug_server_port,
        )

    return app
