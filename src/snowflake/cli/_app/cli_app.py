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
from pathlib import Path
from typing import Optional

import click
import typer
from click import Context as ClickContext
from snowflake.cli import __about__
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
from snowflake.cli.api.config import config_init, get_feature_flags_section
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import CollectionResult
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.config_manager import CONFIG_MANAGER

log = logging.getLogger(__name__)


def _do_not_execute_on_completion(callback):
    def enriched_callback(value):
        if click.get_current_context().resilient_parsing:
            return
        callback(value)

    return enriched_callback


class CliAppFactory:
    def __init__(self):
        self._commands_registration = CommandsRegistrationWithCallbacks()
        self._app: Optional[SnowCliMainTyper] = None
        self._click_context: Optional[ClickContext] = None

    def _exit_with_cleanup(self):
        self._commands_registration.reset_running_instance_registration_state()
        raise typer.Exit()

    def _commands_registration_callback(self):
        def callback(value: bool):
            self._click_context = click.get_current_context()
            if value:
                self._commands_registration.register_commands_from_plugins()
            # required to make the tests working
            # because a single test can execute multiple commands using always the same "app" instance
            self._commands_registration.reset_running_instance_registration_state()

        return callback

    @staticmethod
    def _config_init_callback():
        def callback(configuration_file: Optional[Path]):
            config_init(configuration_file)

        return callback

    def _disable_external_command_plugins_callback(self):
        def callback(value: bool):
            if value:
                self._commands_registration.disable_external_command_plugins()

        return callback

    def _docs_callback(self):
        @_do_not_execute_on_completion
        @self._commands_registration.after
        def callback(value: bool):
            if value:
                ctx = click.get_current_context()
                generate_docs(SecurePath("gen_docs"), ctx.command)
                self._exit_with_cleanup()

        return callback

    def _help_callback(self):
        @_do_not_execute_on_completion
        @self._commands_registration.after
        def callback(value: bool):
            if value:
                ctx = click.get_current_context()
                typer.echo(ctx.get_help())
                self._exit_with_cleanup()

        return callback

    def _commands_structure_callback(self):
        @_do_not_execute_on_completion
        @self._commands_registration.after
        def callback(value: bool):
            if value:
                ctx = click.get_current_context()
                generate_commands_structure(ctx.command).print_node()
                self._exit_with_cleanup()

        return callback

    def _version_callback(self):
        @_do_not_execute_on_completion
        def callback(value: bool):
            if value:
                print_result(
                    MessageResult(f"Snowflake CLI version: {__about__.VERSION}")
                )
                self._exit_with_cleanup()

        return callback

    def _info_callback(self):
        @_do_not_execute_on_completion
        def callback(value: bool):
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
                self._exit_with_cleanup()

        return callback

    def create_or_get_app(self) -> SnowCliMainTyper:
        if self._app:
            return self._app

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
                callback=self._help_callback,
                is_eager=True,
            ),
            version: bool = typer.Option(
                None,
                "--version",
                help="Shows version of the Snowflake CLI",
                callback=self._version_callback(),
                is_eager=True,
            ),
            docs: bool = typer.Option(
                None,
                "--docs",
                hidden=True,
                help="Generates Snowflake CLI documentation",
                callback=self._docs_callback(),
                is_eager=True,
            ),
            structure: bool = typer.Option(
                None,
                "--structure",
                hidden=True,
                help="Prints Snowflake CLI structure of commands",
                callback=self._commands_structure_callback(),
                is_eager=True,
            ),
            info: bool = typer.Option(
                None,
                "--info",
                help="Shows information about the Snowflake CLI",
                callback=self._info_callback(),
            ),
            configuration_file: Path = typer.Option(
                None,
                "--config-file",
                help="Specifies Snowflake CLI configuration file that should be used",
                exists=True,
                dir_okay=False,
                is_eager=True,
                callback=self._config_init_callback(),
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
                callback=self._disable_external_command_plugins_callback(),
                is_eager=True,
                hidden=True,
            ),
            # THIS OPTION SHOULD BE THE LAST OPTION IN THE LIST!
            # ---
            # This is a hidden artificial option used only to guarantee execution of commands registration.
            # This option is also responsible for resetting registration state for test purposes.
            commands_registration: bool = typer.Option(
                True,
                "--commands-registration",
                help="Commands registration",
                hidden=True,
                is_eager=True,
                callback=self._commands_registration_callback(),
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

        self._app = app
        return app

    def get_click_context(self):
        return self._click_context
