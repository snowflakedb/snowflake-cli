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
import contextlib
import dataclasses
import importlib
import json
import locale
import logging
import os
import re
import site
import subprocess
import sys
import tempfile
from functools import cached_property
from pathlib import Path
from typing import Dict, List, Optional

import click
from click import ClickException
from snowflake.cli.api.config import (
    PLUGIN_ENABLED_KEY,
    config_section_exists,
    get_config_section,
    remove_config_path,
    set_config_value,
)
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import PluginNotInstalledError
from snowflake.cli.api.plugins.command import SNOWCLI_COMMAND_PLUGIN_NAMESPACE
from snowflake.cli.api.plugins.plugin_config import PluginConfigProvider
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)
_PLUGIN_INFO_FILENAME = "snowflake_cli_plugin_info.json"
_PYTHONPATH = "PYTHONPATH"


def _pip_install(
    package_name: str, index_url: Optional[str], prefix: Path
) -> subprocess.CompletedProcess:
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        package_name,
        "--prefix",
        str(prefix),
    ]
    if index_url:
        command += ["--index-url", index_url]

    log.info("Running command: %s", " ".join(command))
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding=locale.getpreferredencoding(),
    )
    log.info(
        "pip finished with code %d. Details: %s",
        result.returncode,
        result.stdout + result.stderr,
    )
    if result.returncode != 0:
        raise ClickException(
            f"pip finished with error code {result.returncode}. "
            "Please re-run with --verbose or --debug for more details."
        )
    return result


def _normalize_package_name(package_name: str) -> str:
    """As defined in https://packaging.python.org/en/latest/specifications/name-normalization/"""
    return re.sub(r"[-_.]+", "-", package_name).lower()


@contextlib.contextmanager
def _override_os_pythonpath(pythonpath: Optional[List[str]]):
    old_value = os.environ.get(_PYTHONPATH, None)
    if not pythonpath and old_value:
        del os.environ[_PYTHONPATH]
    elif pythonpath:
        os.environ[_PYTHONPATH] = ":".join(pythonpath)

    yield

    if old_value:
        os.environ[_PYTHONPATH] = old_value
    elif pythonpath:
        del os.environ[_PYTHONPATH]


@contextlib.contextmanager
def _override_sys_path(pythonpath: List[str]):
    old_value = sys.path
    sys.path = pythonpath

    yield

    sys.path = old_value


@dataclasses.dataclass
class PluginInfo:
    """Interface to plugin info file, keeping plugin name - package name mapping."""

    version: int = 1
    plugin_info: Dict[str, str] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_file(cls, file: SecurePath):
        if not file.exists():
            return cls()
        data = json.loads(file.read_text(DEFAULT_SIZE_LIMIT_MB))
        return cls(**data)

    def save_to_file(self, file: SecurePath) -> None:
        file.write_text(json.dumps(self, indent=2))

    def add_plugin(self, plugin_name: str, package_name: str) -> None:
        self.plugin_info[plugin_name] = _normalize_package_name(package_name)

    def remove_plugin(self, plugin_name: str):
        if plugin_name in self.plugin_info:
            del self.plugin_info[plugin_name]

    def get_package_name(self, plugin_name: str) -> Optional[str]:
        return self.plugin_info.get(plugin_name, None)

    def get_plugins_from_package(self, package_name: str) -> List[str]:
        normalized_package_name = _normalize_package_name(package_name)
        return [
            plugin_name
            for plugin_name, plugin_package in self.plugin_info.items()
            if plugin_package == normalized_package_name
        ]


class PluginManager:
    """
    Manages installation of plugins into plugin installation dir and updates config file.
    """

    def __init__(self):
        self._plugin_info = PluginInfo.from_file(self._plugin_info_path)

    def enable_plugin(self, plugin_name: str):
        log.info("Enabling plugin %s in config", plugin_name)
        self._change_plugin_enabled(plugin_name, enable=True)

    def disable_plugin(self, plugin_name: str):
        log.info("Disabling plugin %s in config", plugin_name)
        self._change_plugin_enabled(plugin_name, enable=False)

    @staticmethod
    def _change_plugin_enabled(plugin_name: str, enable: bool):
        plugin_config_path = PluginConfigProvider().plugin_config_section_path(
            plugin_name
        )

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

    @cached_property
    def installation_dir(self):
        return Path(PluginConfigProvider().installation_dir)

    @cached_property
    def _plugin_info_path(self) -> SecurePath:
        return SecurePath(self.installation_dir) / _PLUGIN_INFO_FILENAME

    def _package_site_path(self, package_name: str) -> Path:
        # python library path inside installation directory deduced from base library path
        site_subpath = Path(site.getusersitepackages()).relative_to(site.getuserbase())
        return self._package_installation_dir(package_name) / site_subpath

    def _add_all_installed_packages_to_syspath(self):
        for plugin_dir in self.installation_dir.iterdir():
            if plugin_dir.is_dir():
                plugin_site_path = self._package_site_path(plugin_dir.name)
                if plugin_site_path.exists() and not str(plugin_site_path) in sys.path:
                    site.addsitedir(str(plugin_site_path))

    @staticmethod
    def _detect_plugin_names_from_syspath() -> List[str]:
        return [
            entry_point.name
            for entry_point in importlib.metadata.entry_points(
                group=SNOWCLI_COMMAND_PLUGIN_NAMESPACE
            )
        ]

    def get_installed_plugin_names(self) -> List[str]:
        self._add_all_installed_packages_to_syspath()
        return self._detect_plugin_names_from_syspath()

    def _get_installed_plugins_from_package(self, package_name) -> List[str]:
        package_site_path = self._package_site_path(package_name)
        with _override_sys_path([str(package_site_path)]):
            return self._detect_plugin_names_from_syspath()

    def _check_for_dependency_conflicts(
        self, package_name: str, index_url: Optional[str]
    ) -> None:
        # add all packages to pythonpath for to be detected by pip
        self._add_all_installed_packages_to_syspath()
        with _override_os_pythonpath(sys.path), tempfile.TemporaryDirectory() as tmpdir:
            _pip_install(package_name, index_url, prefix=Path(tmpdir))

    def _package_installation_dir(self, package_name: str) -> Path:
        return self.installation_dir / _normalize_package_name(package_name)

    def _install_package(self, package_name: str, index_url: Optional[str]) -> None:
        # cleanup pythonpath, so new plugin dependencies will be isolated
        plugin_dir = self._package_installation_dir(package_name)
        log.info("Installing package %s into %s", package_name, plugin_dir)
        with _override_os_pythonpath(None):
            _pip_install(package_name, index_url, prefix=plugin_dir)

    def _remove_package(self, package_name: str, missing_ok: bool = True) -> None:
        package_dir = self._package_installation_dir(package_name)
        log.info("Removing package %s located in %s", package_name, package_dir)
        if not missing_ok and not package_dir.exists():
            raise ClickException(f"Package {package_name} is not installed")
        SecurePath(self._package_installation_dir(package_name)).rmdir(
            recursive=True, missing_ok=True
        )

    def _add_new_plugins_to_plugin_info_file(
        self, new_plugins: List[str], package_name: str
    ) -> None:
        for plugin_name in new_plugins:
            self._plugin_info.add_plugin(
                plugin_name=plugin_name, package_name=package_name
            )
        self._plugin_info.save_to_file(self._plugin_info_path)

    def _remove_plugins_from_plugin_info_file(self, removed_plugins: List[str]) -> None:
        for plugin_name in removed_plugins:
            self._plugin_info.remove_plugin(plugin_name)
        self._plugin_info.save_to_file(self._plugin_info_path)

    def _add_new_plugins_to_config(self, new_plugins: List[str]) -> None:
        plugin_config_provider = PluginConfigProvider()
        for plugin_name in new_plugins:
            config_section_path = plugin_config_provider.plugin_config_section_path(
                plugin_name
            )
            if not config_section_exists(config_section_path):
                log.info("Initializing config for plugin %s", plugin_name)
                set_config_value(config_section_path + PLUGIN_ENABLED_KEY, False)

    def _remove_plugins_from_config(self, removed_plugins: List[str]) -> None:
        plugin_config_provider = PluginConfigProvider()
        for plugin_name in removed_plugins:
            config_section_path = plugin_config_provider.plugin_config_section_path(
                plugin_name
            )
            if config_section_exists(plugin_name):
                log.info("Removing config for plugin %s", plugin_name)
                remove_config_path(config_section_path)

    def install_package(self, package_name: str, index_url: Optional[str]) -> List[str]:
        """Installs package into plugin environment. Returns a list of installed plugins."""
        self._assert_not_already_installed(package_name)
        installed_plugins_before = set(self.get_installed_plugin_names())
        self._check_for_dependency_conflicts(package_name, index_url)
        self._install_package(package_name, index_url)
        installed_plugins = self._get_installed_plugins_from_package(package_name)

        if not installed_plugins:
            log.error("No new plugins detected, removing package...")
            self._remove_package(package_name)
            return []

        conflicting_plugins = [
            plugin for plugin in installed_plugins if plugin in installed_plugins_before
        ]
        if conflicting_plugins:
            log.error(
                "Detected plugin name conflicts: %s, removing package...",
                ",".join(conflicting_plugins),
            )
            self._remove_package(package_name)
            raise ClickException(
                f'Package {package_name} contains plugins {",".join(conflicting_plugins)},'
                " which conflicts with already installed plugins."
            )

        self._add_new_plugins_to_plugin_info_file(installed_plugins, package_name)
        self._add_new_plugins_to_config(installed_plugins)
        return installed_plugins

    def uninstall(self, plugin_name: str) -> List[str]:
        """Uninstall package from plugin environment and removes its config section (if it exists).
        Returns list of removed plugins names."""
        package_name = self._plugin_info.get_package_name(plugin_name)
        plugins_from_package = self._plugin_info.get_plugins_from_package(package_name)
        if not click.confirm(
            f"This will uninstall package {package_name} containing plugins {','.join(plugins_from_package)}."
            " Do you want to continue?"
        ):
            return []

        installed_plugins_before = self.get_installed_plugin_names()
        self._remove_package(package_name)
        installed_plugins_after = set(self.get_installed_plugin_names())
        actually_removed_plugins = [
            name
            for name in installed_plugins_before
            if name not in installed_plugins_after
        ]
        self._remove_plugins_from_plugin_info_file(actually_removed_plugins)
        self._remove_plugins_from_config(actually_removed_plugins)
        return actually_removed_plugins

    def _assert_not_already_installed(self, package_name):
        if self._package_installation_dir(package_name).exists():
            raise ClickException(f"Package {package_name} is already installed.")
