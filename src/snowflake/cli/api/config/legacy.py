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

"""
Legacy configuration system.

This module contains the original configuration handling code that was previously
in snowflake.cli.api.config. It provides backward compatibility while allowing
for a gradual migration to the new config-ng system.
"""

from __future__ import annotations

import logging
import os
import warnings
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import tomlkit
from click import ClickException
from snowflake.cli.api.exceptions import (
    ConfigFileTooWidePermissionsError,
    MissingConfigurationError,
    UnsupportedConfigSectionTypeError,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.secure_utils import (
    file_permissions_are_strict,
    windows_get_not_whitelisted_users_with_access,
)
from snowflake.cli.api.utils.dict_utils import remove_key_from_nested_dict_if_exists
from snowflake.cli.api.utils.types import try_cast_to_bool
from snowflake.connector.compat import IS_WINDOWS
from snowflake.connector.config_manager import CONFIG_MANAGER
from snowflake.connector.errors import ConfigSourceError, MissingConfigOptionError
from tomlkit import TOMLDocument, dump
from tomlkit.container import Container
from tomlkit.exceptions import NonExistentKey
from tomlkit.items import Table

log = logging.getLogger(__name__)


class Empty:
    pass


# Configuration section constants
CONNECTIONS_SECTION = "connections"
CLI_SECTION = "cli"
LOGS_SECTION = "logs"
PLUGINS_SECTION = "plugins"
IGNORE_NEW_VERSION_WARNING_KEY = "ignore_new_version_warning"

# Configuration path constants
LOGS_SECTION_PATH = [CLI_SECTION, LOGS_SECTION]
PLUGINS_SECTION_PATH = [CLI_SECTION, PLUGINS_SECTION]
PLUGIN_ENABLED_KEY = "enabled"
FEATURE_FLAGS_SECTION_PATH = [CLI_SECTION, "features"]

# Initialize CONFIG_MANAGER with CLI section
CONFIG_MANAGER.add_option(
    name=CLI_SECTION,
    parse_str=tomlkit.parse,
    default=dict(),
)


class ConfigManagerWrapper:
    """
    Wrapper around CONFIG_MANAGER that provides testable interface.

    This allows for dependency injection and clean test isolation
    without relying on module reloading or singleton state management.
    """

    def __init__(self, config_manager=None):
        self._config_manager = config_manager or CONFIG_MANAGER

    def __getitem__(self, key):
        """Delegate dict-like access to the underlying config manager."""
        return self._config_manager[key]

    def __setitem__(self, key, value):
        """Delegate dict-like assignment to the underlying config manager."""
        self._config_manager[key] = value

    @property
    def file_path(self):
        """Get the config file path."""
        return self._config_manager.file_path

    @file_path.setter
    def file_path(self, value):
        """Set the config file path."""
        self._config_manager.file_path = value

    @property
    def conf_file_cache(self):
        """Get the config file cache."""
        return self._config_manager.conf_file_cache

    @conf_file_cache.setter
    def conf_file_cache(self, value):
        """Set the config file cache."""
        self._config_manager.conf_file_cache = value

    def read_config(self):
        """Read configuration from files."""
        return self._config_manager.read_config()

    def use_default_paths(self, temp_dir: Path):
        """Set config manager to use default paths in the test directory."""
        from snowflake.connector.constants import CONFIG_FILE

        self._config_manager.file_path = CONFIG_FILE

    def force_reload(self):
        """Force CONFIG_MANAGER to reload configuration from disk."""
        # Clear all cached state to force reload
        self._config_manager.conf_file_cache = None

        # Clear any cached option values
        if hasattr(self._config_manager, "_options"):
            for option in getattr(self._config_manager, "_options", {}).values():
                if hasattr(option, "_cached_value"):
                    setattr(option, "_cached_value", None)
                if hasattr(option, "_value"):
                    setattr(option, "_value", None)

        # Clear slices and re-initialize them to detect connections.toml
        setattr(self._config_manager, "_slices", [])

        # Re-add connections slice if it exists
        from snowflake.connector.constants import CONNECTIONS_FILE

        if CONNECTIONS_FILE.exists():
            from snowflake.connector.config_manager import (
                ConfigSlice,
                ConfigSliceOptions,
            )

            _slices = getattr(self._config_manager, "_slices", []) or []
            _slices.append(
                ConfigSlice(
                    path=CONNECTIONS_FILE,
                    options=ConfigSliceOptions(
                        check_permissions=True, only_in_slice=False
                    ),
                    section="connections",
                )
            )
            setattr(self._config_manager, "_slices", _slices)

        # Force re-reading configuration
        self._config_manager.read_config()

    def reset_for_testing(self, temp_dir: Path):
        """
        Reset config manager state for testing with isolated paths.

        This provides clean isolation without module reloading.
        """
        # Clear all cached state
        self._config_manager.conf_file_cache = None

        # Clear slices and reset with test paths
        setattr(self._config_manager, "_slices", [])

        # Reset cached option values
        if hasattr(self._config_manager, "_options"):
            for option in getattr(self._config_manager, "_options", {}).values():
                if hasattr(option, "_cached_value"):
                    setattr(option, "_cached_value", None)
                if hasattr(option, "_value"):
                    setattr(option, "_value", None)

        # Set file_path to default for isolated tests (like permission tests)
        # Tests that need specific config files should not use isolated_config
        from snowflake.connector.constants import CONFIG_FILE

        self._config_manager.file_path = CONFIG_FILE

        # Re-add connections slice with test path
        connections_file = temp_dir / "connections.toml"
        from snowflake.connector.config_manager import ConfigSlice, ConfigSliceOptions

        _slices = [
            ConfigSlice(
                path=connections_file,
                options=ConfigSliceOptions(check_permissions=True, only_in_slice=False),
                section="connections",
            )
        ]
        setattr(self._config_manager, "_slices", _slices)


# Global config manager instance - can be replaced for testing
_current_config_manager = ConfigManagerWrapper()


def get_config_manager() -> ConfigManagerWrapper:
    """Get the current config manager instance."""
    return _current_config_manager


def set_config_manager_for_testing(config_manager: ConfigManagerWrapper):
    """Replace the global config manager for testing."""
    global _current_config_manager
    _current_config_manager = config_manager


def force_config_reload():
    """Force the global config manager to reload configuration from disk."""
    get_config_manager().force_reload()


def reset_config_manager_completely():
    """
    Completely reset CONFIG_MANAGER state by clearing all caches.

    This is a more aggressive reset for tests that need completely clean state.
    """
    # Get the current config manager
    config_manager = get_config_manager()
    internal_config_manager = getattr(config_manager, "_config_manager")

    # Clear all cached state
    setattr(internal_config_manager, "conf_file_cache", None)
    setattr(internal_config_manager, "_slices", [])

    # Clear cached option values
    if hasattr(internal_config_manager, "_options"):
        for option in getattr(internal_config_manager, "_options").values():
            if hasattr(option, "_cached_value"):
                setattr(option, "_cached_value", None)
            if hasattr(option, "_value"):
                setattr(option, "_value", None)

    # Clear sub-managers cache to force reconstruction
    if hasattr(internal_config_manager, "_sub_managers"):
        getattr(internal_config_manager, "_sub_managers").clear()

    # Force a fresh read of configuration
    internal_config_manager.read_config()


@contextmanager
def isolated_config(temp_dir: Path):
    """
    Context manager that provides isolated config for testing.

    This eliminates the need for module reloading and complex state management.

    Usage:
        with isolated_config(tmp_path / ".snowflake"):
            # All config operations are isolated to temp_dir
            config_init(None)
            set_config_value(["default_connection_name"], "test")
    """
    # Create a fresh wrapper for testing
    test_wrapper = ConfigManagerWrapper()
    original_wrapper = get_config_manager()

    try:
        # Set up isolated environment
        test_wrapper.reset_for_testing(temp_dir)
        set_config_manager_for_testing(test_wrapper)

        # Don't override with monitoring wrapper - let tests set their own config file paths
        # The key isolation is in the reset_for_testing method

        yield test_wrapper
    finally:
        # Restore original config manager
        set_config_manager_for_testing(original_wrapper)


@dataclass
class ConnectionConfig:
    """Configuration for Snowflake connections."""

    account: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = field(default=None, repr=False)
    host: Optional[str] = None
    region: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    authenticator: Optional[str] = None
    workload_identity_provider: Optional[str] = None
    private_key_file: Optional[str] = None
    token_file_path: Optional[str] = None
    oauth_client_id: Optional[str] = None
    oauth_client_secret: Optional[str] = None
    oauth_authorization_url: Optional[str] = None
    oauth_token_request_url: Optional[str] = None
    oauth_redirect_uri: Optional[str] = None
    oauth_scope: Optional[str] = None
    oatuh_enable_pkce: Optional[bool] = None
    oauth_enable_refresh_tokens: Optional[bool] = None
    oauth_enable_single_use_refresh_tokens: Optional[bool] = None
    client_store_temporary_credential: Optional[bool] = None

    _other_settings: dict = field(default_factory=lambda: {})

    @classmethod
    def from_dict(cls, config_dict: dict) -> ConnectionConfig:
        """Create ConnectionConfig from dictionary."""
        known_settings = {}
        other_settings = {}
        for key, value in config_dict.items():
            if key in cls.__dict__:
                known_settings[key] = value
            else:
                other_settings[key] = value
        return cls(**known_settings, _other_settings=other_settings)

    def to_dict_of_known_non_empty_values(self) -> dict:
        """Convert to dictionary with only known non-empty values."""
        return {
            k: v
            for k, v in asdict(self).items()
            if k != "_other_settings" and v is not None
        }

    def _non_empty_other_values(self) -> dict:
        """Get non-empty other settings."""
        return {k: v for k, v in self._other_settings.items() if v is not None}

    def to_dict_of_all_non_empty_values(self) -> dict:
        """Convert to dictionary with all non-empty values."""
        return {
            **self.to_dict_of_known_non_empty_values(),
            **self._non_empty_other_values(),
        }


# Default configuration values
_DEFAULT_LOGS_CONFIG = {
    "save_logs": True,
    "path": str(get_config_manager().file_path.parent / "logs"),
    "level": "info",
}

_DEFAULT_CLI_CONFIG = {LOGS_SECTION: _DEFAULT_LOGS_CONFIG}


def config_init(config_file: Optional[Path]) -> None:
    """
    Initialize the app configuration.

    Config provided via cli flag takes precedence.
    If config file does not exist we create an empty one.
    """
    from snowflake.cli._app.loggers import create_initial_loggers

    config_manager = get_config_manager()
    if config_file:
        config_manager.file_path = config_file
    else:
        _check_default_config_files_permissions()
    if not config_manager.file_path.exists():
        _initialise_config(config_manager.file_path)
    _read_config_file()
    create_initial_loggers()


def add_connection_to_proper_file(
    name: str, connection_config: ConnectionConfig
) -> Path:
    """Add connection to the appropriate configuration file."""
    from snowflake.connector.constants import CONNECTIONS_FILE

    if CONNECTIONS_FILE.exists():
        existing_connections = _read_connections_toml()
        existing_connections.update(
            {name: connection_config.to_dict_of_all_non_empty_values()}
        )
        _update_connections_toml(existing_connections)
        return CONNECTIONS_FILE
    else:
        set_config_value(
            path=[CONNECTIONS_SECTION, name],
            value=connection_config.to_dict_of_all_non_empty_values(),
        )
        return get_config_manager().file_path


def remove_connection_from_proper_file(name: str) -> Path:
    """Remove connection from the appropriate configuration file."""
    from snowflake.connector.constants import CONNECTIONS_FILE

    if CONNECTIONS_FILE.exists():
        existing_connections = _read_connections_toml()
        if name not in existing_connections:
            raise MissingConfigurationError(f"Connection {name} is not configured")
        del existing_connections[name]
        _update_connections_toml(existing_connections)
        return CONNECTIONS_FILE
    else:
        unset_config_value(path=[CONNECTIONS_SECTION, name])
        return get_config_manager().file_path


@contextmanager
def _config_file():
    """Context manager for configuration file operations."""
    _read_config_file()
    config_manager = get_config_manager()
    conf_file_cache = config_manager.conf_file_cache
    yield conf_file_cache
    _dump_config(conf_file_cache)


def _read_config_file() -> None:
    """Read and parse the configuration file."""
    with warnings.catch_warnings():
        if IS_WINDOWS:
            warnings.filterwarnings(
                action="ignore",
                message="Bad owner or permissions.*",
                module="snowflake.connector.config_manager",
            )

            config_manager = get_config_manager()
            if not file_permissions_are_strict(config_manager.file_path):
                users = ", ".join(
                    windows_get_not_whitelisted_users_with_access(
                        config_manager.file_path
                    )
                )
                warnings.warn(
                    f"Unauthorized users ({users}) have access to configuration file {config_manager.file_path}.\n"
                    f'Run `icacls "{config_manager.file_path}" /remove:g <USER_ID>` on those users to restrict permissions.'
                )

        try:
            config_manager = get_config_manager()
            config_manager.read_config()
        except ConfigSourceError as exception:
            raise ClickException(
                f"Configuration file seems to be corrupted. {str(exception.__cause__)}"
            )


def _initialise_logs_section() -> None:
    """Initialize the logs section with default values."""
    with _config_file() as conf_file_cache:
        conf_file_cache[CLI_SECTION][LOGS_SECTION] = _DEFAULT_LOGS_CONFIG


def _initialise_cli_section() -> None:
    """Initialize the CLI section with default values."""
    with _config_file() as conf_file_cache:
        conf_file_cache[CLI_SECTION] = {IGNORE_NEW_VERSION_WARNING_KEY: False}


def set_config_value(path: List[str], value: Any) -> None:
    """
    Set value in config.

    For example to set value "val" to key "key" in section [a.b.c], call
    set_config_value(["a", "b", "c", "key"], "val").
    If you want to override a whole section, value should be a dictionary.
    """
    with _config_file() as conf_file_cache:
        current_config_dict = conf_file_cache
        for key in path[:-1]:
            if key not in current_config_dict:
                current_config_dict[key] = {}
            current_config_dict = current_config_dict[key]
        current_config_dict[path[-1]] = value


def unset_config_value(path: List[str]) -> None:
    """
    Unset value in config.

    For example to unset value for key "key" in section [a.b.c], call
    unset_config_value(["a", "b", "c", "key"]).
    """
    with _config_file() as conf_file_cache:
        remove_key_from_nested_dict_if_exists(conf_file_cache, path)


def get_logs_config() -> dict:
    """Get logs configuration with defaults."""
    logs_config = _DEFAULT_LOGS_CONFIG.copy()
    if config_section_exists(*LOGS_SECTION_PATH):
        logs_config.update(**get_config_section(*LOGS_SECTION_PATH))
    return logs_config


def get_plugins_config() -> dict:
    """Get plugins configuration."""
    if config_section_exists(*PLUGINS_SECTION_PATH):
        return get_config_section(*PLUGINS_SECTION_PATH)
    else:
        return {}


def connection_exists(connection_name: str) -> bool:
    """Check if a connection exists in configuration."""
    return config_section_exists(CONNECTIONS_SECTION, connection_name)


def config_section_exists(*path) -> bool:
    """Check if a configuration section exists."""
    try:
        _find_section(*path)
        return True
    except (KeyError, NonExistentKey, MissingConfigOptionError):
        return False


def get_all_connections() -> dict[str, ConnectionConfig]:
    """Get all configured connections."""
    return {
        k: ConnectionConfig.from_dict(connection_dict)
        for k, connection_dict in get_config_section("connections").items()
    }


def get_connection_dict(connection_name: str) -> dict:
    """Get connection configuration as dictionary."""
    try:
        return get_config_section(CONNECTIONS_SECTION, connection_name)
    except KeyError:
        raise MissingConfigurationError(
            f"Connection {connection_name} is not configured"
        )


def get_default_connection_name() -> str:
    """Get the default connection name."""
    return get_config_manager()["default_connection_name"]


def get_default_connection_dict() -> dict:
    """Get the default connection configuration."""
    def_connection_name = get_default_connection_name()
    if not connection_exists(def_connection_name):
        raise MissingConfigurationError(
            f"Couldn't find connection for default connection `{def_connection_name}`. "
            f"Specify connection name or configure default connection."
        )
    return get_connection_dict(def_connection_name)


def get_config_section(*path) -> dict:
    """Get a configuration section."""
    section = _find_section(*path)
    if isinstance(section, Container):
        return {s: _merge_section_with_env(section[s], *path, s) for s in section}
    if isinstance(section, dict):
        return _merge_section_with_env(section, *path)
    raise UnsupportedConfigSectionTypeError(type(section))


def get_config_value(*path, key: str, default: Optional[Any] = Empty) -> Any:
    """Look for given key under nested path in toml file."""
    env_variable = get_env_value(*path, key=key)
    if env_variable:
        return env_variable
    try:
        return get_config_section(*path)[key]
    except (KeyError, NonExistentKey, MissingConfigOptionError, ConfigSourceError):
        if default is not Empty:
            return default
        raise


def get_config_bool_value(*path, key: str, default: Optional[bool]) -> Optional[bool]:
    """Get a boolean configuration value."""
    value = get_config_value(*path, key=key, default=None)

    if value is None:
        return default

    try:
        return try_cast_to_bool(value)
    except ValueError:
        raise ClickException(
            f"Expected boolean value for {'.'.join((*path, key))} option."
        )


def get_env_variable_name(*path, key: str) -> str:
    """Generate environment variable name for configuration path."""
    return ("_".join(["snowflake", *path, key])).upper()


def get_env_value(*path, key: str) -> str | None:
    """Get environment variable value for configuration path."""
    return os.environ.get(get_env_variable_name(*path, key=key))


def get_feature_flags_section() -> Dict[str, bool | Literal["UNKNOWN"]]:
    """Get feature flags configuration."""
    if not config_section_exists(*FEATURE_FLAGS_SECTION_PATH):
        return {}

    flags = get_config_section(*FEATURE_FLAGS_SECTION_PATH)

    def _bool_or_unknown(value):
        try:
            return try_cast_to_bool(value)
        except ValueError:
            return "UNKNOWN"

    return {k: _bool_or_unknown(v) for k, v in flags.items()}


# Private helper functions


def _initialise_config(config_file: Path) -> None:
    """Initialize a new configuration file."""
    config_file = SecurePath(config_file)
    config_file.parent.mkdir(parents=True, exist_ok=True)
    config_file.touch()
    _initialise_cli_section()
    _initialise_logs_section()
    log.info(
        "Created Snowflake configuration file at %s", get_config_manager().file_path
    )


def _find_section(*path) -> TOMLDocument:
    """Find a configuration section by path."""
    section = get_config_manager()
    idx = 0
    while idx < len(path):
        section = section[path[idx]]
        idx += 1
    return section


def _merge_section_with_env(section: Union[Table, Any], *path) -> Dict[str, str]:
    """Merge configuration section with environment variables."""
    if isinstance(section, Table):
        env_variables = _get_envs_for_path(*path)
        section_copy = section.copy()
        section_copy.update(env_variables)
        return section_copy.unwrap()
    # It's an atomic value
    return section


def _get_envs_for_path(*path) -> dict:
    """Get environment variables for a configuration path."""
    env_variables_prefix = "_".join(["SNOWFLAKE"] + [p.upper() for p in path]) + "_"
    return {
        k.replace(env_variables_prefix, "").lower(): os.environ[k]
        for k in os.environ.keys()
        if k.startswith(env_variables_prefix)
    }


def _dump_config(config_and_connections: Dict) -> None:
    """Dump configuration to files."""
    from snowflake.connector.constants import CONNECTIONS_FILE

    config_toml_dict = config_and_connections.copy()

    if CONNECTIONS_FILE.exists():
        # update connections in connections.toml
        # it will add only connections (maybe updated) which were originally read from connections.toml
        # it won't add connections from config.toml
        # because config manager doesn't have connections from config.toml if connections.toml exists
        _update_connections_toml(config_and_connections.get("connections") or {})
        # to config.toml save only connections from config.toml
        connections_to_save_in_config_toml = _read_config_file_toml().get("connections")
        if connections_to_save_in_config_toml:
            config_toml_dict["connections"] = connections_to_save_in_config_toml
        else:
            config_toml_dict.pop("connections", None)

    with SecurePath(get_config_manager().file_path).open("w+") as fh:
        dump(config_toml_dict, fh)


def _check_default_config_files_permissions() -> None:
    """Check permissions on default configuration files."""
    # Import constants inside the function to get updated values after module reload
    from snowflake.connector.constants import CONFIG_FILE, CONNECTIONS_FILE

    if not IS_WINDOWS:
        if CONNECTIONS_FILE.exists() and not file_permissions_are_strict(
            CONNECTIONS_FILE
        ):
            raise ConfigFileTooWidePermissionsError(CONNECTIONS_FILE)
        if CONFIG_FILE.exists() and not file_permissions_are_strict(CONFIG_FILE):
            raise ConfigFileTooWidePermissionsError(CONFIG_FILE)


def _read_config_file_toml() -> dict:
    """Read configuration file as TOML."""
    return tomlkit.loads(get_config_manager().file_path.read_text()).unwrap()


def _read_connections_toml() -> dict:
    """Read connections file as TOML."""
    from snowflake.connector.constants import CONNECTIONS_FILE

    return tomlkit.loads(CONNECTIONS_FILE.read_text()).unwrap()


def _update_connections_toml(connections: dict) -> None:
    """Update connections TOML file."""
    from snowflake.connector.constants import CONNECTIONS_FILE

    with open(CONNECTIONS_FILE, "w") as f:
        f.write(tomlkit.dumps(connections))
