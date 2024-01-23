from typing import List

from snowflake.cli.api import PluginConfigProvider
from snowflake.cli.api.config import (
    PLUGINS_SECTION_PATH,
    config_section_exists,
    get_config_section,
    get_config_value,
    get_plugins_config,
)
from snowflake.cli.api.plugins.plugin_config import PluginConfig


class PluginConfigProviderImpl(PluginConfigProvider):
    def get_enabled_plugin_names(self) -> List[str]:
        enabled_plugins = []
        for (plugin_name, plugin_config_section) in get_plugins_config().items():
            if plugin_config_section.get("enabled", False):
                enabled_plugins.append(plugin_name)
        return enabled_plugins

    def get_config(self, plugin_name: str) -> PluginConfig:
        config_path = PLUGINS_SECTION_PATH + [plugin_name]
        plugin_config = PluginConfig(is_plugin_enabled=False, internal_config={})
        plugin_config.is_plugin_enabled = get_config_value(
            *config_path, key="enabled", default=False
        )
        if config_section_exists(*config_path, "config"):
            plugin_config.internal_config = get_config_section(*config_path, "config")
        return plugin_config
