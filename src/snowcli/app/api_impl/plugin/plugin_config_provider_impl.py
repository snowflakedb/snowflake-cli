from typing import List

from snowcli.api import PluginConfigProvider
from snowcli.api.plugin.plugin_config import PluginConfig
from snowcli.config import cli_config


class PluginConfigProviderImpl(PluginConfigProvider):
    def get_enabled_plugin_names(self) -> List[str]:
        config_path = ["snowcli", "plugins"]
        enabled_plugins = []
        if cli_config.section_exists(*config_path):
            for (plugin_name, plugin_config_section) in cli_config.get_section(
                *config_path
            ).items():
                if plugin_config_section.get("enabled", default=False):
                    enabled_plugins.append(plugin_name)
        return enabled_plugins

    def get_config(self, plugin_name: str) -> PluginConfig:
        config_path = ["snowcli", "plugins", plugin_name]
        plugin_config = PluginConfig(is_plugin_enabled=False, internal_config={})
        plugin_config.is_plugin_enabled = cli_config.get(
            *config_path, key="enabled", default=False
        )
        if cli_config.section_exists(*config_path, "config"):
            plugin_config.internal_config = cli_config.get_section(
                *config_path, "config"
            )
        return plugin_config
