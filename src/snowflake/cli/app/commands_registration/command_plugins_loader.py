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
from typing import Dict, List, Optional

import pluggy
from snowflake.cli.api.plugins.command import (
    SNOWCLI_COMMAND_PLUGIN_NAMESPACE,
    CommandPath,
    CommandSpec,
    plugin_hook_specs,
)
from snowflake.cli.app.commands_registration import (
    LoadedBuiltInCommandPlugin,
    LoadedCommandPlugin,
    LoadedExternalCommandPlugin,
)
from snowflake.cli.app.commands_registration.builtin_plugins import (
    get_builtin_plugin_name_to_plugin_spec,
)
from snowflake.cli.app.commands_registration.exception_logging import exception_logging

log = logging.getLogger(__name__)
log_exception = exception_logging(log)


class CommandPluginsLoader:
    def __init__(self):
        plugin_manager = pluggy.PluginManager(SNOWCLI_COMMAND_PLUGIN_NAMESPACE)
        plugin_manager.add_hookspecs(plugin_hook_specs)
        self._plugin_manager = plugin_manager
        self._loaded_plugins: Dict[str, LoadedCommandPlugin] = {}
        self._loaded_command_paths: Dict[CommandPath, LoadedCommandPlugin] = {}

    def register_builtin_plugins(self) -> None:
        for plugin_name, plugin in get_builtin_plugin_name_to_plugin_spec().items():
            try:
                self._plugin_manager.register(plugin=plugin, name=plugin_name)
            except Exception as ex:
                log_exception(
                    f"Cannot register plugin [{plugin_name}]: {ex.__str__()}", ex
                )

    def register_external_plugins(self, plugin_names: List[str]) -> None:
        for plugin_name in plugin_names:
            try:
                self._plugin_manager.load_setuptools_entrypoints(
                    SNOWCLI_COMMAND_PLUGIN_NAMESPACE, plugin_name
                )
            except Exception as ex:
                log_exception(
                    f"Cannot register plugin [{plugin_name}]: {ex.__str__()}", ex
                )

    def load_all_registered_plugins(self) -> List[LoadedCommandPlugin]:
        for plugin_name, plugin in self._plugin_manager.list_name_plugin():
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
                "Cannot load plugin [%s] "
                "because it defines the same command [%s] "
                "as already loaded plugin [%s].",
                plugin_name,
                loaded_plugin.command_spec.full_command_path,
                other_plugin_with_the_same_command_path.plugin_name,
            )
            return None
        self._loaded_plugins[plugin_name] = loaded_plugin
        self._loaded_command_paths[
            loaded_plugin.command_spec.full_command_path
        ] = loaded_plugin

        if self._is_external_plugin(loaded_plugin):
            log.info("Loaded external plugin: %s", plugin_name)

        return loaded_plugin

    def _load_plugin_spec(
        self, plugin_name: str, plugin
    ) -> Optional[LoadedCommandPlugin]:
        if plugin_name in get_builtin_plugin_name_to_plugin_spec().keys():
            return self._load_builtin_plugin_spec(plugin_name, plugin)
        else:
            return self._load_external_plugin_spec(plugin_name, plugin)

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

    def _load_external_plugin_spec(
        self, plugin_name: str, plugin
    ) -> Optional[LoadedCommandPlugin]:
        command_spec = self._load_command_spec(plugin_name, plugin)
        if command_spec:
            return LoadedExternalCommandPlugin(
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

    @staticmethod
    def _is_external_plugin(plugin) -> bool:
        return isinstance(plugin, LoadedExternalCommandPlugin)


def load_only_builtin_command_plugins() -> List[LoadedCommandPlugin]:
    loader = CommandPluginsLoader()
    loader.register_builtin_plugins()
    return loader.load_all_registered_plugins()


def load_builtin_and_external_command_plugins(
    external_plugin_names: List[str],
) -> List[LoadedCommandPlugin]:
    loader = CommandPluginsLoader()
    loader.register_builtin_plugins()
    loader.register_external_plugins(external_plugin_names)
    return loader.load_all_registered_plugins()
