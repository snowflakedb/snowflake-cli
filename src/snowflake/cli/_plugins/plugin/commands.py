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
import subprocess
import sys

import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.plugins.plugin_config import PluginConfigManager

log = logging.getLogger(__name__)

app = SnowTyperFactory(
    name="plugin",
    help="Plugin management commands.",
)


@app.command(name="install", requires_connection=False)
def install(
    package: str = typer.Argument(
        None, help="Pip compatible package (PyPI name / URL / local path)"
    ),
    **options,
) -> CommandResult:
    """Installs a plugin from a package"""
    target_dir: str = (
        "/tmp/cli_plugins"  # TODO make it configurable in config.toml with some default
    )
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--prefix",
            target_dir,
            "--upgrade",
            package,
        ]
    )
    return MessageResult(f"Plugin successfully installed.")


@app.command(name="enable", requires_connection=False)
def enable(
    plugin_name: str = typer.Argument(None, help="Plugin name"),
    **options,
) -> CommandResult:
    """Enables a plugin with a given name."""
    plugin_config_manager = PluginConfigManager()
    plugin_config_manager.enable_plugin(plugin_name)

    return MessageResult(f"Plugin {plugin_name} successfully enabled.")


@app.command(name="disable", requires_connection=False)
def disable(
    plugin_name: str = typer.Argument(None, help="Plugin name"),
    **options,
) -> CommandResult:
    """Disables a plugin with a given name."""
    plugin_config_manager = PluginConfigManager()
    plugin_config_manager.disable_plugin(plugin_name)

    return MessageResult(f"Plugin {plugin_name} successfully disabled.")
