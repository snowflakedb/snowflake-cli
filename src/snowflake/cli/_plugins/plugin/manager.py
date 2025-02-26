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
import locale
import logging
import subprocess
from typing import List

from click import ClickException
from snowflake.cli.api.config import (
    PLUGIN_ENABLED_KEY,
    PLUGINS_SECTION_PATH,
    config_section_exists,
    get_config_section,
    set_config_value,
    get_plugins_config,
)
from snowflake.cli.api.exceptions import PluginNotInstalledError
from snowflake.cli.api.plugins.command import SNOWCLI_COMMAND_PLUGIN_NAMESPACE
from snowflake.cli.api.plugins.plugin_config import PluginConfigProvider

log = logging.getLogger(__name__)


def _pip(command: List[str], raise_on_error: bool) -> subprocess.CompletedProcess:
    import sys

    command = [sys.executable, "-m", "pip"] + command

    log.info("Running command: %s", " ".join(command))
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding=locale.getpreferredencoding(),
    )
    log.info(
        "pip finished with error code %d. Details: %s",
        result.returncode,
        result.stdout + result.stderr,
    )
    if raise_on_error and result.returncode != 0:
        raise ClickException(
            f"pip finished with error code {result.returncode}. "
            "Please re-run with --verbose or --debug for more details."
        )
    return result


class PluginManager:
    """
    Manage installation of plugins.
    """

    def enable_plugin(self, plugin_name: str):
        log.info("Enabling plugin %s in config", plugin_name)
        self._change_plugin_enabled(plugin_name, enable=True)

    def disable_plugin(self, plugin_name: str):
        log.info("Disabling plugin %s in config", plugin_name)
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

    def install_package(self, package_name: str, index_url: str) -> None:
        """Installs package into plugin environment."""
        installation_dir = PluginConfigProvider().installation_dir
        _pip(
            [
                "install",
                package_name,
                "--prefix",
                installation_dir,
                "--index-url",
                index_url,
            ],
            raise_on_error=True,
        )

    @staticmethod
    def _remove_plugin_from_config(plugin_name) -> None:
        log.info("Removing plugin %s from config", plugin_name)
        plugins_config = get_plugins_config()
        if plugin_name in plugins_config:
            del plugins_config[plugin_name]
            set_config_value(PLUGINS_SECTION_PATH, plugins_config)

    def uninstall_package(self, package_name: str) -> None:
        """Uninstall package from plugin environment and removes its config section (if it exists)."""
        _pip(["uninstall", package_name, "-y"], raise_on_error=True)
        self._remove_plugin_from_config(package_name)

    @staticmethod
    def is_package_installed(package_name: str) -> bool:
        result = _pip(["show", package_name], raise_on_error=False)
        return result.returncode == 0
