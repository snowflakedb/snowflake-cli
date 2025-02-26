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
from typing import Dict, List

import typer
from snowflake.cli._plugins.plugin.manager import PluginManager
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
)

log = logging.getLogger(__name__)

app = SnowTyperFactory(
    name="plugin",
    help="Plugin management commands.",
    is_hidden=lambda: True,
)


@app.command(name="enable", requires_connection=False)
def enable(
    plugin_name: str = typer.Argument(None, help="Plugin name"),
    **options,
) -> CommandResult:
    """Enables a plugin with a given name."""
    plugin_manager = PluginManager()
    plugin_manager.assert_plugin_is_installed(plugin_name)
    plugin_manager.enable_plugin(plugin_name)

    return MessageResult(f"Plugin {plugin_name} successfully enabled.")


@app.command(name="disable", requires_connection=False)
def disable(
    plugin_name: str = typer.Argument(None, help="Plugin name"),
    **options,
) -> CommandResult:
    """Disables a plugin with a given name."""
    plugin_manager = PluginManager()
    plugin_manager.assert_plugin_is_installed(plugin_name)
    plugin_manager.disable_plugin(plugin_name)

    return MessageResult(f"Plugin {plugin_name} successfully disabled.")


@app.command(name="list", requires_connection=False)
def list_(
    **options,
) -> CommandResult:
    """Lists all installed plugins."""
    plugin_manager = PluginManager()
    result: List[Dict[str, str]] = []
    for plugin_name in sorted(plugin_manager.get_installed_plugin_names()):
        result.append(
            {
                "plugin name": plugin_name,
                "enabled": plugin_manager.is_plugin_enabled(plugin_name),
            }
        )

    return CollectionResult(result)
