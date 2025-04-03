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

from dataclasses import dataclass
from typing import Any, Dict, List

from snowflake.cli.api.config import (
    PLUGIN_ENABLED_KEY,
    PLUGINS_SECTION_PATH,
    config_section_exists,
    get_config_section,
    get_config_value,
    get_plugins_config,
)
from snowflake.cli.api.exceptions import InvalidPluginConfigurationError


@dataclass
class PluginConfig:
    is_plugin_enabled: bool
    internal_config: Dict[str, Any]


class PluginConfigProvider:
    @staticmethod
    def get_enabled_plugin_names() -> List[str]:
        enabled_plugins = []
        for plugin_name, plugin_config_section in get_plugins_config().items():
            enabled = plugin_config_section.get(PLUGIN_ENABLED_KEY, False)
            _assert_value_is_bool(
                enabled, value_name=PLUGIN_ENABLED_KEY, plugin_name=plugin_name
            )
            if enabled:
                enabled_plugins.append(plugin_name)
        return enabled_plugins

    @staticmethod
    def get_config(plugin_name: str) -> PluginConfig:
        config_path = PLUGINS_SECTION_PATH + [plugin_name]
        plugin_config = PluginConfig(is_plugin_enabled=False, internal_config={})
        plugin_config.is_plugin_enabled = get_config_value(
            *config_path, key=PLUGIN_ENABLED_KEY, default=False
        )
        _assert_value_is_bool(
            plugin_config.is_plugin_enabled,
            value_name=PLUGIN_ENABLED_KEY,
            plugin_name=plugin_name,
        )
        if config_section_exists(*config_path, "config"):
            plugin_config.internal_config = get_config_section(*config_path, "config")
        return plugin_config


def _assert_value_is_bool(value, *, value_name: str, plugin_name: str) -> None:
    if type(value) is not bool:
        raise InvalidPluginConfigurationError(
            f'[{plugin_name}]: "{value_name}" must be a boolean'
        )
