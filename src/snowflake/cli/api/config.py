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
import os
import warnings
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    Final,
    List,
    Literal,
    Optional,
    Union,
)

import tomlkit
from click import ClickException
from snowflake.cli.api.exceptions import (
    ConfigFileTooWidePermissionsError,
    MissingConfigurationError,
    UnsupportedConfigSectionTypeError,
)
from snowflake.cli.api.sanitizers import sanitize_source_error
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.secure_utils import (
    file_permissions_are_strict,
    windows_get_not_whitelisted_users_with_access,
)
from snowflake.cli.api.utils.dict_utils import remove_key_from_nested_dict_if_exists
from snowflake.cli.api.utils.path_utils import path_resolver
from snowflake.cli.api.utils.types import try_cast_to_bool
from snowflake.connector.compat import IS_WINDOWS
from snowflake.connector.constants import CONFIG_FILE
from snowflake.connector.errors import ConfigSourceError, MissingConfigOptionError
from tomlkit import TOMLDocument, dump
from tomlkit.container import Container
from tomlkit.exceptions import NonExistentKey
from tomlkit.items import Table

log = logging.getLogger(__name__)


def get_connections_file():
    """
    Dynamically get the current CONNECTIONS_FILE path.
    This ensures we get the updated value after module reloads in tests.
    """
    from snowflake.connector.constants import CONNECTIONS_FILE as _CONNECTIONS_FILE

    return _CONNECTIONS_FILE


def get_config_manager():
    """
    Get the current configuration manager from CLI context.
    This replaces direct CONFIG_MANAGER access throughout the codebase.
    """
    from snowflake.cli.api.cli_global_context import get_cli_context_manager

    return get_cli_context_manager().config_manager


class Empty:
    pass


CONNECTIONS_SECTION = "connections"
CLI_SECTION = "cli"
LOGS_SECTION = "logs"
PLUGINS_SECTION = "plugins"
IGNORE_NEW_VERSION_WARNING_KEY = "ignore_new_version_warning"

LOGS_SECTION_PATH = [CLI_SECTION, LOGS_SECTION]
PLUGINS_SECTION_PATH = [CLI_SECTION, PLUGINS_SECTION]
PLUGIN_ENABLED_KEY = "enabled"
FEATURE_FLAGS_SECTION_PATH = [CLI_SECTION, "features"]


LEGACY_OAUTH_PKCE_KEY: Literal["oatuh_enable_pkce"] = "oatuh_enable_pkce"
LEGACY_CONNECTION_SETTING_ALIASES: Final[dict[str, str]] = {
    LEGACY_OAUTH_PKCE_KEY: "oauth_enable_pkce",
}


@dataclass
class ConnectionConfig:
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
    private_key_raw: Optional[str] = field(default=None, repr=False)
    private_key_passphrase: Optional[str] = field(default=None, repr=False)
    token: Optional[str] = field(default=None, repr=False)
    session_token: Optional[str] = field(default=None, repr=False)
    master_token: Optional[str] = field(default=None, repr=False)
    token_file_path: Optional[str] = None
    oauth_client_id: Optional[str] = None
    oauth_client_secret: Optional[str] = None
    oauth_authorization_url: Optional[str] = None
    oauth_token_request_url: Optional[str] = None
    oauth_redirect_uri: Optional[str] = None
    oauth_scope: Optional[str] = None
    oauth_enable_pkce: Optional[bool] = None
    oauth_enable_refresh_tokens: Optional[bool] = None
    oauth_enable_single_use_refresh_tokens: Optional[bool] = None
    client_store_temporary_credential: Optional[bool] = None

    _other_settings: dict = field(default_factory=lambda: {})

    @classmethod
    def from_dict(cls, config_dict: dict) -> ConnectionConfig:
        known_settings = {}
        other_settings = {}
        for key, value in config_dict.items():
            normalized_key = cls._normalize_setting_key(key)
            if normalized_key in cls.__dict__:
                known_settings[normalized_key] = value
            else:
                other_settings[key] = value
        return cls(**known_settings, _other_settings=other_settings)

    @staticmethod
    def _normalize_setting_key(key: str) -> str:
        return LEGACY_CONNECTION_SETTING_ALIASES.get(key, key)

    def to_dict_of_known_non_empty_values(self) -> dict:
        return {
            k: v
            for k, v in asdict(self).items()
            if k != "_other_settings" and v is not None
        }

    def _non_empty_other_values(self) -> dict:
        return {k: v for k, v in self._other_settings.items() if v is not None}

    def to_dict_of_all_non_empty_values(self) -> dict:
        return {
            **self.to_dict_of_known_non_empty_values(),
            **self._non_empty_other_values(),
        }


def config_init(config_file: Optional[Path]):
    """
    Initializes the app configuration. Config provided via cli flag takes precedence.
    If config file does not exist we create an empty one.
    """
    from snowflake.cli._app.loggers import create_initial_loggers
    from snowflake.cli.api.cli_global_context import get_cli_context_manager

    if config_file:
        get_cli_context_manager().config_file_override = config_file
        _check_custom_config_permissions(config_file)
    else:
        _check_default_config_files_permissions()

    config_manager = get_config_manager()
    if not config_manager.file_path.exists():
        _initialise_config(config_manager.file_path)
    _read_config_file()
    create_initial_loggers()


def add_connection_to_proper_file(name: str, connection_config: ConnectionConfig):
    connections_file = get_connections_file()
    if connections_file.exists():
        existing_connections = _read_connections_toml()
        existing_connections.update(
            {name: connection_config.to_dict_of_all_non_empty_values()}
        )
        _update_connections_toml(existing_connections)
        return connections_file
    else:
        set_config_value(
            path=[CONNECTIONS_SECTION, name],
            value=connection_config.to_dict_of_all_non_empty_values(),
        )
        return get_config_manager().file_path


def remove_connection_from_proper_file(name: str):
    connections_file = get_connections_file()
    if connections_file.exists():
        existing_connections = _read_connections_toml()
        if name not in existing_connections:
            raise MissingConfigurationError(f"Connection {name} is not configured")
        del existing_connections[name]
        _update_connections_toml(existing_connections)
        return connections_file
    else:
        unset_config_value(path=[CONNECTIONS_SECTION, name])
        return get_config_manager().file_path


def _get_default_logs_config() -> dict:
    """Get default logs configuration with lazy evaluation to avoid circular imports."""
    config_parent_path = get_config_manager().file_path.parent
    resolved_parent_path = path_resolver(str(config_parent_path))

    return {
        "save_logs": True,
        "path": str(Path(resolved_parent_path) / "logs"),
        "level": "info",
    }


def _get_default_cli_config() -> dict:
    """Get default CLI configuration with lazy evaluation."""
    return {LOGS_SECTION: _get_default_logs_config()}


@contextmanager
def _config_file():
    _read_config_file()
    config_manager = get_config_manager()
    conf_file_cache = config_manager.conf_file_cache
    yield conf_file_cache
    _dump_config(conf_file_cache)

    # Reset config provider cache after writing to ensure it re-reads on next access
    try:
        from snowflake.cli.api.config_provider import get_config_provider_singleton

        provider = get_config_provider_singleton()
        if hasattr(provider, "invalidate_cache"):
            provider.invalidate_cache()
    except Exception as exc:
        sanitized_error = sanitize_source_error(exc)
        log.error("Failed to invalidate configuration cache: %s", sanitized_error)
        raise


def _warn_about_wide_permissions_on_custom_config(config_path: Path) -> None:
    """Issue a warning for custom config files with wide permissions (Unix only)."""
    if file_permissions_are_strict(config_path):
        return

    warnings.warn(
        f"Bad owner or permissions on {config_path}.\n"
        f' * To change owner, run `chown $USER "{config_path}"`.\n'
        f' * To restrict permissions, run `chmod 0600 "{config_path}"`.\n'
        f" * In future versions of Snowflake CLI strict configuration file permissions will be mandatory. "
        f"To test if your files have correct permissions set SNOWFLAKE_CLI_FEATURES_ENFORCE_STRICT_CONFIG_PERMISSIONS=1 and try again."
    )


def _read_config_file():
    from snowflake.cli.api.cli_global_context import get_cli_context_manager

    config_manager = get_config_manager()
    is_custom_config = get_cli_context_manager().config_file_override is not None
    skip_permissions_check = is_custom_config

    with warnings.catch_warnings():
        if IS_WINDOWS:
            _handle_windows_permission_warning(config_manager.file_path)
        elif is_custom_config:
            skip_permissions_check = not _handle_unix_custom_config_permissions(
                config_manager.file_path
            )

        try:
            config_manager.read_config(
                skip_file_permissions_check=skip_permissions_check
            )
        except ConfigSourceError as exception:
            error_detail = (
                str(exception.__cause__) if exception.__cause__ else exception.msg
            )
            raise ClickException(
                f"Configuration file seems to be corrupted. {error_detail}"
            )


def _handle_windows_permission_warning(config_path: Path) -> None:
    """Handle permission warnings for Windows."""
    warnings.filterwarnings(
        action="ignore",
        message="Bad owner or permissions.*",
        module="snowflake.connector.config_manager",
    )

    if not file_permissions_are_strict(config_path):
        users = ", ".join(windows_get_not_whitelisted_users_with_access(config_path))
        warnings.warn(
            f"Unauthorized users ({users}) have access to configuration file {config_path}.\n"
            f'Run `icacls "{config_path}" /remove:g <USER_ID>` on those users to restrict permissions.'
        )


def _handle_unix_custom_config_permissions(config_path: Path) -> bool:
    """
    Handle permission checks for custom config files on Unix.
    """
    if _enforce_strict_config_permissions():
        return True

    _warn_about_wide_permissions_on_custom_config(config_path)
    return False


def _initialise_logs_section():
    with _config_file() as conf_file_cache:
        conf_file_cache[CLI_SECTION][LOGS_SECTION] = _get_default_logs_config()


def _initialise_cli_section():
    with _config_file() as conf_file_cache:
        conf_file_cache[CLI_SECTION] = {IGNORE_NEW_VERSION_WARNING_KEY: False}


def set_config_value(path: List[str], value: Any) -> None:
    """Sets value in config.
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
    """Unsets value in config.
    For example to unset value for key "key" in section [a.b.c], call
    unset_config_value(["a", "b", "c", "key"]).
    """
    with _config_file() as conf_file_cache:
        remove_key_from_nested_dict_if_exists(conf_file_cache, path)


def get_logs_config() -> dict:
    logs_config = _get_default_logs_config().copy()
    if config_section_exists(*LOGS_SECTION_PATH):
        logs_config.update(**get_config_section(*LOGS_SECTION_PATH))
    return logs_config


def get_plugins_config() -> dict:
    if config_section_exists(*PLUGINS_SECTION_PATH):
        return get_config_section(*PLUGINS_SECTION_PATH)
    else:
        return {}


def connection_exists(connection_name: str) -> bool:
    return config_section_exists(CONNECTIONS_SECTION, connection_name)


def config_section_exists(*path) -> bool:
    try:
        _find_section(*path)
        return True
    except (KeyError, NonExistentKey, MissingConfigOptionError):
        return False


def get_all_connections() -> dict[str, ConnectionConfig]:
    from snowflake.cli.api.config_provider import get_config_provider_singleton

    provider = get_config_provider_singleton()
    return provider.get_all_connections()


def get_connection_dict(connection_name: str) -> dict:
    from snowflake.cli.api.config_provider import get_config_provider_singleton

    provider = get_config_provider_singleton()
    connection_raw = provider.get_connection_dict(connection_name)
    connection = ConnectionConfig.from_dict(connection_raw)
    return connection.to_dict_of_all_non_empty_values()


def get_default_connection_name() -> str:
    return get_config_manager()["default_connection_name"]


def get_default_connection_dict() -> dict:
    def_connection_name = get_default_connection_name()
    if not connection_exists(def_connection_name):
        raise MissingConfigurationError(
            f"Couldn't find connection for default connection `{def_connection_name}`. "
            f"Specify connection name or configure default connection."
        )
    return get_connection_dict(def_connection_name)


def get_config_section(*path) -> dict:
    section = _find_section(*path)
    if isinstance(section, Container):
        return {s: _merge_section_with_env(section[s], *path, s) for s in section}
    if isinstance(section, dict):
        return _merge_section_with_env(section, *path)
    raise UnsupportedConfigSectionTypeError(type(section))


def get_config_value(*path, key: str, default: Optional[Any] = Empty) -> Any:
    """Looks for given key under nested path in toml file."""
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
    value = get_config_value(*path, key=key, default=None)

    if value is None:
        return default

    try:
        return try_cast_to_bool(value)
    except ValueError:
        raise ClickException(
            f"Expected boolean value for {'.'.join((*path, key))} option."
        )


def _initialise_config(config_file: Path) -> None:
    config_file = SecurePath(config_file)
    config_file.parent.mkdir(parents=True, exist_ok=True)
    config_file.touch()
    _initialise_cli_section()
    _initialise_logs_section()
    log.info(
        "Created Snowflake configuration file at %s", get_config_manager().file_path
    )


def get_env_variable_name(*path, key: str) -> str:
    return ("_".join(["snowflake", *path, key])).upper()


def get_env_value(*path, key: str) -> str | None:
    return os.environ.get(get_env_variable_name(*path, key=key))


def _find_section(*path) -> TOMLDocument:
    section = get_config_manager()
    idx = 0
    while idx < len(path):
        section = section[path[idx]]
        idx += 1
    return section


def _merge_section_with_env(section: Union[Table, Any], *path) -> Dict[str, str]:
    if isinstance(section, Table):
        env_variables = _get_envs_for_path(*path)
        section_copy = section.copy()
        section_copy.update(env_variables)
        return section_copy.unwrap()
    # It's a atomic value
    return section


def _get_envs_for_path(*path) -> dict:
    env_variables_prefix = "_".join(["SNOWFLAKE"] + [p.upper() for p in path]) + "_"
    return {
        k.replace(env_variables_prefix, "").lower(): os.environ[k]
        for k in os.environ.keys()
        if k.startswith(env_variables_prefix)
    }


def _dump_config(config_and_connections: Dict):
    config_toml_dict = config_and_connections.copy()

    connections_file = get_connections_file()
    if connections_file.exists():
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
    if not IS_WINDOWS:
        connections_file = get_connections_file()
        if connections_file.exists() and not file_permissions_are_strict(
            connections_file
        ):
            raise ConfigFileTooWidePermissionsError(connections_file)
        if CONFIG_FILE.exists() and not file_permissions_are_strict(CONFIG_FILE):
            raise ConfigFileTooWidePermissionsError(CONFIG_FILE)


def _check_custom_config_permissions(config_file: Path) -> None:
    """
    Check custom config file permissions if ENFORCE_STRICT_CONFIG_PERMISSIONS flag is enabled.

    This allows users to opt-in to strict permission checking on custom config files.
    The flag can be set via environment variable SNOWFLAKE_CLI_FEATURES_ENFORCE_STRICT_CONFIG_PERMISSIONS.
    """
    if IS_WINDOWS:
        return

    if (
        _enforce_strict_config_permissions()
        and config_file.exists()
        and not file_permissions_are_strict(config_file)
    ):
        raise ConfigFileTooWidePermissionsError(config_file)


def _enforce_strict_config_permissions() -> bool:
    env_var = get_env_variable_name(
        *FEATURE_FLAGS_SECTION_PATH, key="ENFORCE_STRICT_CONFIG_PERMISSIONS"
    )
    return os.environ.get(env_var, "").lower() in ("true", "1", "yes", "on")


def get_feature_flags_section() -> Dict[str, bool | Literal["UNKNOWN"]]:
    if not config_section_exists(*FEATURE_FLAGS_SECTION_PATH):
        return {}

    flags = get_config_section(*FEATURE_FLAGS_SECTION_PATH)

    def _bool_or_unknown(value):
        try:
            return try_cast_to_bool(value)
        except ValueError:
            return "UNKNOWN"

    return {k: _bool_or_unknown(v) for k, v in flags.items()}


def _read_config_file_toml() -> dict:
    return tomlkit.loads(get_config_manager().file_path.read_text()).unwrap()


def _read_connections_toml() -> dict:
    return tomlkit.loads(get_connections_file().read_text()).unwrap()


def _update_connections_toml(connections: dict):
    with open(get_connections_file(), "w") as f:
        f.write(tomlkit.dumps(connections))
