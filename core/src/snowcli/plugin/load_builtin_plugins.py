import logging
from typing import List

import pluggy

import snowcli.cli.stage.manager
from snowcli.cli.common.snow_cli_global_context import global_context_copy
from snowcli.cli.stage.stage_builtin_plugin_hook_impl import plugin_spec
from snowcli.plugin import BuiltInLoadedPlugin
from snowcli.plugin.api import plugin_hook_specs

log = logging.getLogger(__name__)


class LoadBuiltInPlugins:

    _builtin_plugins = {"stage": snowcli.cli.stage.stage_builtin_plugin_hook_impl}

    def __init__(self):
        plugin_manager = pluggy.PluginManager("snowcli.plugin")
        plugin_manager.add_hookspecs(plugin_hook_specs)
        self._plugin_manager = plugin_manager

    def __call__(self, *args, **kwargs) -> List[BuiltInLoadedPlugin]:
        self._load_plugins_code_to_manager()
        return self._extract_info_and_specifications_of_loaded_plugins()

    def _load_plugins_code_to_manager(self) -> None:
        for (plugin_name, plugin) in self._builtin_plugins.items():
            self._plugin_manager.register(plugin=plugin, name=plugin_name)

    def _extract_info_and_specifications_of_loaded_plugins(
        self,
    ) -> List[BuiltInLoadedPlugin]:
        plugins = []
        for (plugin_name, plugin) in self._plugin_manager.list_name_plugin():
            try:
                plugins.append(
                    BuiltInLoadedPlugin(
                        plugin_name=plugin_name,
                        plugin_spec=plugin.plugin_spec(),
                    )
                )
            except Exception as ex:
                log.error(
                    msg=f"Cannot load info about built-in plugin [{plugin_name}]: {ex.__str__()}",
                    exc_info=global_context_copy().debug,
                )
        return plugins
