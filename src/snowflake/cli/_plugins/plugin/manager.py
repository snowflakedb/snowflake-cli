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
import importlib
from typing import List

from snowflake.cli.api.config import (
    PLUGIN_ENABLED_KEY,
    PLUGINS_SECTION_PATH,
    config_section_exists,
    get_config_section,
    set_config_value,
)
from snowflake.cli.api.exceptions import PluginNotInstalledError
from snowflake.cli.api.plugins.command import SNOWCLI_COMMAND_PLUGIN_NAMESPACE
from snowflake.cli.api.plugins.plugin_config import PluginConfigProvider


class PluginManager:
    """
    Manage installation of plugins.
    """

    def enable_plugin(self, plugin_name: str):
        self._change_plugin_enabled(plugin_name, enable=True)

    def disable_plugin(self, plugin_name: str):
        self._change_plugin_enabled(plugin_name, enable=False)

    @staticmethod
    def _change_plugin_enabled(plugin_name: str, enable: bool):
        plugin_config_path = PLUGINS_SECTION_PATH + [plugin_name]

        if config_section_exists(*plugin_config_path):
            plugin_config = get_config_section(*plugin_config_path)
        elif enable:
            plugin_config = {}
        else:
            # do not add a new plugin config if user wants to disable a plugin which is not configured
            # (plugins are disabled by default)
            return

        plugin_config[PLUGIN_ENABLED_KEY] = enable
        set_config_value(path=plugin_config_path, value=plugin_config)

    @staticmethod
    def is_plugin_enabled(plugin_name: str) -> bool:
        return PluginConfigProvider.get_config(plugin_name).is_plugin_enabled

    def assert_plugin_is_installed(self, plugin_name: str):
        installed_plugins = self.get_installed_plugin_names()
        if plugin_name not in installed_plugins:
            raise PluginNotInstalledError(
                plugin_name, installed_plugins=sorted(installed_plugins)
            )

    @staticmethod
    def get_installed_plugin_names() -> List[str]:
        return [
            entry_point.name
            for entry_point in importlib.metadata.entry_points(
                group=SNOWCLI_COMMAND_PLUGIN_NAMESPACE
            )
        ]
