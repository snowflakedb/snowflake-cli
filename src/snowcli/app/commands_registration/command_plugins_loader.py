import logging
from typing import List, Optional, Dict

import pluggy

from snowcli.api.plugin.command import (
    SNOWCLI_COMMAND_PLUGIN_NAMESPACE,
    plugin_hook_specs,
    CommandSpec,
    CommandPath,
)
from snowcli.app.commands_registration import (
    LoadedCommandPlugin,
    LoadedBuiltInCommandPlugin,
)
from snowcli.app.commands_registration.builtin_plugins import (
    builtin_plugin_name_to_plugin_spec,
)
from snowcli.cli.exception_logging import exception_logging

log = logging.getLogger(__name__)
log_exception = exception_logging(log)


class CommandPluginsLoader:
    def __init__(self):
        plugin_manager = pluggy.PluginManager(SNOWCLI_COMMAND_PLUGIN_NAMESPACE)
        plugin_manager.add_hookspecs(plugin_hook_specs)
        self._plugin_manager = plugin_manager
        self._loaded_plugins: Dict[str, LoadedCommandPlugin] = {}
        self._loaded_command_paths: Dict[CommandPath, LoadedCommandPlugin] = {}

    def register_only_builtin_plugins(self) -> None:
        for (plugin_name, plugin) in builtin_plugin_name_to_plugin_spec.items():
            try:
                self._plugin_manager.register(plugin=plugin, name=plugin_name)
            except Exception as ex:
                log_exception(
                    f"Cannot register plugin [{plugin_name}]: {ex.__str__()}", ex
                )

    def load_all_registered_plugins(self) -> List[LoadedCommandPlugin]:
        for (plugin_name, plugin) in self._plugin_manager.list_name_plugin():
            self._load_plugin(plugin_name, plugin)
        return list(self._loaded_plugins.values())

    def _load_plugin(self, plugin_name: str, plugin) -> Optional[LoadedCommandPlugin]:
        already_loaded_plugin = self._loaded_plugins.get(plugin_name)
        if already_loaded_plugin:
            return already_loaded_plugin
        return self._load_new_plugin(plugin_name, plugin)

    def _load_new_plugin(
        self, plugin_name: str, plugin
    ) -> Optional[LoadedCommandPlugin]:
        loaded_plugin = self._load_plugin_spec(plugin_name, plugin)
        if not loaded_plugin:
            return None
        other_plugin_with_the_same_command_path = self._loaded_command_paths.get(
            loaded_plugin.command_spec.full_command_path
        )
        if other_plugin_with_the_same_command_path:
            log.error(
                f"Cannot load plugin [{plugin_name}] "
                f"because it defines the same command [{loaded_plugin.command_spec.full_command_path}] "
                f"as already loaded plugin [{other_plugin_with_the_same_command_path.plugin_name}]."
            )
            return None
        self._loaded_plugins[plugin_name] = loaded_plugin
        self._loaded_command_paths[
            loaded_plugin.command_spec.full_command_path
        ] = loaded_plugin
        return loaded_plugin

    def _load_plugin_spec(
        self, plugin_name: str, plugin
    ) -> Optional[LoadedCommandPlugin]:
        if plugin_name in builtin_plugin_name_to_plugin_spec.keys():
            return self._load_builtin_plugin_spec(plugin_name, plugin)
        else:
            log.error(f"Unsupported type of plugin with name [{plugin_name}]")
            return None

    def _load_builtin_plugin_spec(
        self, plugin_name: str, plugin
    ) -> Optional[LoadedCommandPlugin]:
        command_spec = self._load_command_spec(plugin_name, plugin)
        if command_spec:
            return LoadedBuiltInCommandPlugin(
                plugin_name=plugin_name,
                command_spec=command_spec,
            )
        else:
            return None

    @staticmethod
    def _load_command_spec(plugin_name: str, plugin) -> Optional[CommandSpec]:
        try:
            return plugin.command_spec()
        except Exception as ex:
            log_exception(
                f"Cannot load command specification from plugin [{plugin_name}]: {ex.__str__()}",
                ex,
            )
            return None


def load_only_builtin_command_plugins() -> List[LoadedCommandPlugin]:
    loader = CommandPluginsLoader()
    loader.register_only_builtin_plugins()
    return loader.load_all_registered_plugins()
