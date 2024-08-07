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

from typing import List

from snowflake.cli.api import PluginConfigProvider
from snowflake.cli.api.config import (
    PLUGINS_SECTION_PATH,
    config_section_exists,
    get_config_section,
    get_config_value,
    get_plugins_config,
)
from snowflake.cli.api.exceptions import InvalidPluginConfiguration
from snowflake.cli.api.plugins.plugin_config import PluginConfig


def _assert_value_is_bool(value, *, value_name: str, plugin_name: str) -> None:
    if type(value) is not bool:
        raise InvalidPluginConfiguration(
            f'[{plugin_name}]: "{value_name}" must be a boolean'
        )


_ENABLED_KEY = "enabled"


class PluginConfigProviderImpl(PluginConfigProvider):
    def get_enabled_plugin_names(self) -> List[str]:
        enabled_plugins = []
        for plugin_name, plugin_config_section in get_plugins_config().items():
            enabled = plugin_config_section.get(_ENABLED_KEY, False)
            _assert_value_is_bool(
                enabled, value_name=_ENABLED_KEY, plugin_name=plugin_name
            )
            if enabled:
                enabled_plugins.append(plugin_name)
        return enabled_plugins

    def get_config(self, plugin_name: str) -> PluginConfig:
        config_path = PLUGINS_SECTION_PATH + [plugin_name]
        plugin_config = PluginConfig(is_plugin_enabled=False, internal_config={})
        plugin_config.is_plugin_enabled = get_config_value(
            *config_path, key=_ENABLED_KEY, default=False
        )
        _assert_value_is_bool(
            plugin_config.is_plugin_enabled,
            value_name=_ENABLED_KEY,
            plugin_name=plugin_name,
        )
        if config_section_exists(*config_path, "config"):
            plugin_config.internal_config = get_config_section(*config_path, "config")
        return plugin_config
