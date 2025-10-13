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

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from snowflake.cli.api.config_ng.resolver import ConfigurationResolver

ALTERNATIVE_CONFIG_ENV_VAR = "SNOWFLAKE_CLI_CONFIG_V2_ENABLED"


class ConfigProvider(ABC):
    """
    Abstract base class for configuration providers.
    All methods must return data in the same format as current implementation.
    """

    @abstractmethod
    def get_section(self, *path) -> dict:
        """Get configuration section at specified path."""
        ...

    @abstractmethod
    def get_value(self, *path, key: str, default: Optional[Any] = None) -> Any:
        """Get single configuration value."""
        ...

    @abstractmethod
    def set_value(self, path: list[str], value: Any) -> None:
        """Set configuration value at path."""
        ...

    @abstractmethod
    def unset_value(self, path: list[str]) -> None:
        """Remove configuration value at path."""
        ...

    @abstractmethod
    def section_exists(self, *path) -> bool:
        """Check if configuration section exists."""
        ...

    @abstractmethod
    def read_config(self) -> None:
        """Load configuration from source."""
        ...

    @abstractmethod
    def get_connection_dict(self, connection_name: str) -> dict:
        """Get connection configuration by name."""
        ...

    @abstractmethod
    def get_all_connections(self, include_env_connections: bool = False) -> dict:
        """Get all connection configurations.

        Args:
            include_env_connections: If True, include connections created from
                                    environment variables. Default False.
        """
        ...

    def _transform_private_key_raw(self, connection_dict: dict) -> dict:
        """
        Transform private_key_raw to private_key_file for ConnectionContext compatibility.

        The ConnectionContext dataclass doesn't have a private_key_raw field, so it gets
        filtered out by merge_with_config. To work around this, we write private_key_raw
        content to a temporary file and return it as private_key_file.

        Args:
            connection_dict: Connection configuration dictionary

        Returns:
            Modified connection dictionary with private_key_raw transformed to private_key_file
        """
        if "private_key_raw" not in connection_dict:
            return connection_dict

        # Don't transform if private_key_file is already set
        if "private_key_file" in connection_dict:
            return connection_dict

        import os
        import tempfile

        try:
            # Create a temporary file with the private key content
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".pem", delete=False
            ) as f:
                f.write(connection_dict["private_key_raw"])
                temp_file_path = f.name

            # Set restrictive permissions on the temporary file
            os.chmod(temp_file_path, 0o600)

            # Create a copy of the connection dict with the transformation
            result = connection_dict.copy()
            result["private_key_file"] = temp_file_path
            del result["private_key_raw"]

            return result

        except Exception:
            # If transformation fails, return original dict
            # The error will be handled downstream
            return connection_dict


class LegacyConfigProvider(ConfigProvider):
    """
    Current TOML-based configuration provider.
    Wraps existing implementation for compatibility.
    """

    def get_section(self, *path) -> dict:
        from snowflake.cli.api.config import get_config_section

        return get_config_section(*path)

    def get_value(self, *path, key: str, default: Optional[Any] = None) -> Any:
        from snowflake.cli.api.config import Empty, get_config_value

        return get_config_value(
            *path, key=key, default=default if default is not None else Empty
        )

    def set_value(self, path: list[str], value: Any) -> None:
        from snowflake.cli.api.config import set_config_value

        set_config_value(path, value)

    def unset_value(self, path: list[str]) -> None:
        from snowflake.cli.api.config import unset_config_value

        unset_config_value(path)

    def section_exists(self, *path) -> bool:
        from snowflake.cli.api.config import config_section_exists

        return config_section_exists(*path)

    def read_config(self) -> None:
        from snowflake.cli.api.config import get_config_manager

        config_manager = get_config_manager()
        config_manager.read_config()

    def get_connection_dict(self, connection_name: str) -> dict:
        from snowflake.cli.api.config import get_config_section

        try:
            result = get_config_section("connections", connection_name)
            return self._transform_private_key_raw(result)
        except KeyError:
            from snowflake.cli.api.exceptions import MissingConfigurationError

            raise MissingConfigurationError(
                f"Connection {connection_name} is not configured"
            )

    def get_all_connections(self, include_env_connections: bool = False) -> dict:
        from snowflake.cli.api.config import get_all_connections

        # Legacy provider ignores the flag since it never had env connections
        return get_all_connections()


class AlternativeConfigProvider(ConfigProvider):
    """
    New configuration provider using config_ng resolution system.

    This provider uses ConfigurationResolver to discover values from:
    - CLI arguments (highest priority)
    - Environment variables (SNOWFLAKE_* and SNOWSQL_*)
    - Configuration files (SnowCLI TOML and SnowSQL config)

    Maintains backward compatibility with LegacyConfigProvider output format.
    """

    def __init__(self) -> None:
        self._resolver: Optional[ConfigurationResolver] = None
        self._config_cache: Dict[str, Any] = {}
        self._initialized: bool = False
        self._last_config_override: Optional[Path] = None

    def _ensure_initialized(self) -> None:
        """Lazily initialize the resolver on first use."""
        # Check if config_file_override has changed
        try:
            from snowflake.cli.api.cli_global_context import get_cli_context

            current_override = get_cli_context().config_file_override

            # If override changed, force re-initialization
            if current_override != self._last_config_override:
                self._initialized = False
                self._config_cache.clear()
                self._last_config_override = current_override
        except Exception:
            pass

        if self._initialized:
            return

        from snowflake.cli.api.cli_global_context import get_cli_context
        from snowflake.cli.api.config_ng import (
            CliConfigFile,
            CliEnvironment,
            CliParameters,
            ConfigurationResolver,
            ConnectionsConfigFile,
            SnowSQLConfigFile,
            SnowSQLEnvironment,
        )

        # Get CLI context safely
        try:
            cli_context = get_cli_context().connection_context
            cli_context_dict = cli_context.present_values_as_dict()
        except Exception:
            cli_context_dict = {}

        # Create sources in precedence order (lowest to highest priority)
        # File sources return keys: connections.{name}.{param}
        # Env/CLI sources return flat keys: account, user, etc.

        sources = [
            # 1. SnowSQL config files (lowest priority, merged)
            SnowSQLConfigFile(),
            # 2. CLI config.toml (first-found behavior)
            CliConfigFile(),
            # 3. Dedicated connections.toml
            ConnectionsConfigFile(),
            # 4. SnowSQL environment variables (SNOWSQL_*)
            SnowSQLEnvironment(),
            # 5. CLI environment variables (SNOWFLAKE_*)
            CliEnvironment(),
            # 6. CLI command-line arguments (highest priority)
            CliParameters(cli_context=cli_context_dict),
        ]

        # Create resolver with all sources in order
        self._resolver = ConfigurationResolver(sources=sources, track_history=True)

        self._initialized = True

    def read_config(self) -> None:
        """
        Load configuration from all sources.
        For config_ng, this means (re)initializing the resolver.
        """
        self._initialized = False
        self._config_cache.clear()
        self._last_config_override = None  # Reset cached override to force re-check
        self._ensure_initialized()

        # Resolve all configuration to populate cache
        assert self._resolver is not None
        self._config_cache = self._resolver.resolve()

    def get_section(self, *path) -> dict:
        """
        Get configuration section at specified path.

        Args:
            *path: Section path (e.g., "connections", "my_conn")

        Returns:
            Dictionary of section contents
        """
        self._ensure_initialized()

        if not self._config_cache:
            assert self._resolver is not None
            self._config_cache = self._resolver.resolve()

        # Navigate through path to find section
        if not path:
            return self._config_cache

        # For connections section, return all connections as nested dicts
        if len(path) == 1 and path[0] == "connections":
            return self._get_all_connections_dict()

        # For specific connection, return connection dict
        if len(path) == 2 and path[0] == "connections":
            connection_name = path[1]
            return self._get_connection_dict_internal(connection_name)

        # For other sections, try to resolve with path prefix
        section_prefix = ".".join(path)
        result = {}
        for key, value in self._config_cache.items():
            if key.startswith(section_prefix + "."):
                # Strip prefix to get relative key
                relative_key = key[len(section_prefix) + 1 :]
                result[relative_key] = value
            elif key == section_prefix:
                # Exact match for section itself
                return value if isinstance(value, dict) else {section_prefix: value}

        return result

    def get_value(self, *path, key: str, default: Optional[Any] = None) -> Any:
        """
        Get single configuration value at path + key.

        Args:
            *path: Path to section
            key: Configuration key
            default: Default value if not found

        Returns:
            Configuration value or default
        """
        self._ensure_initialized()

        if not self._config_cache:
            assert self._resolver is not None
            self._config_cache = self._resolver.resolve()

        # Build full key from path and key
        if path:
            full_key = ".".join(path) + "." + key
        else:
            full_key = key

        # Try to resolve the value
        value = self._config_cache.get(full_key, default)
        return value

    def set_value(self, path: list[str], value: Any) -> None:
        """
        Set configuration value at path.

        Note: config_ng is read-only for resolution. This delegates to
        legacy config system for writing.
        """
        from snowflake.cli.api.config import set_config_value as legacy_set_value

        legacy_set_value(path, value)
        # Clear cache to force re-read on next access
        self._config_cache.clear()
        self._initialized = False

    def unset_value(self, path: list[str]) -> None:
        """
        Remove configuration value at path.

        Note: config_ng is read-only for resolution. This delegates to
        legacy config system for writing.
        """
        from snowflake.cli.api.config import unset_config_value as legacy_unset_value

        legacy_unset_value(path)
        # Clear cache to force re-read on next access
        self._config_cache.clear()
        self._initialized = False

    def section_exists(self, *path) -> bool:
        """
        Check if configuration section exists.

        Args:
            *path: Section path

        Returns:
            True if section exists and has values
        """
        self._ensure_initialized()

        if not self._config_cache:
            assert self._resolver is not None
            self._config_cache = self._resolver.resolve()

        if not path:
            return True

        section_prefix = ".".join(path)
        # Check if any key starts with this prefix
        return any(
            key == section_prefix or key.startswith(section_prefix + ".")
            for key in self._config_cache.keys()
        )

    def _get_connection_dict_internal(self, connection_name: str) -> Dict[str, Any]:
        """
        Get connection configuration by name.

        Behavior is controlled by SNOWFLAKE_CLI_CONNECTIONS_TOML_REPLACE environment variable:
        - If set to "true" (default): connections.toml completely replaces connections
          from config.toml (legacy behavior)
        - If set to "false": connections.toml values are merged with config.toml values

        Args:
            connection_name: Name of the connection

        Returns:
            Dictionary of connection parameters from file sources only
        """
        self._ensure_initialized()

        if not self._config_cache:
            assert self._resolver is not None
            self._config_cache = self._resolver.resolve()

        connection_dict: Dict[str, Any] = {}
        connection_prefix = f"connections.{connection_name}."

        # Check if replacement behavior is enabled (default: true for backward compatibility)
        import os

        replace_behavior = os.environ.get(
            "SNOWFLAKE_CLI_CONNECTIONS_TOML_REPLACE", "true"
        ).lower() in ("true", "1", "yes", "on")

        if replace_behavior:
            # Legacy replacement behavior: if connections.toml has the connection,
            # use ONLY values from connections.toml
            has_connections_toml = False
            if self._resolver is not None:
                for key in self._config_cache.keys():
                    if key.startswith(connection_prefix):
                        # Check resolution history to see if this came from connections.toml
                        history = self._resolver.get_resolution_history(key)
                        if history and history.selected_entry:
                            if (
                                history.selected_entry.config_value.source_name
                                == "connections_toml"
                            ):
                                has_connections_toml = True
                                break

            if has_connections_toml:
                # Use ONLY connections.toml values (replacement behavior)
                for key, value in self._config_cache.items():
                    if key.startswith(connection_prefix):
                        # Check if this specific value comes from connections.toml
                        if self._resolver is not None:
                            history = self._resolver.get_resolution_history(key)
                            if history and history.selected_entry:
                                if (
                                    history.selected_entry.config_value.source_name
                                    == "connections_toml"
                                ):
                                    param_name = key[len(connection_prefix) :]
                                    connection_dict[param_name] = value
            else:
                # No connections.toml, use merged values from other sources
                for key, value in self._config_cache.items():
                    if key.startswith(connection_prefix):
                        param_name = key[len(connection_prefix) :]
                        connection_dict[param_name] = value
        else:
            # New merging behavior: merge all sources normally
            for key, value in self._config_cache.items():
                if key.startswith(connection_prefix):
                    param_name = key[len(connection_prefix) :]
                    connection_dict[param_name] = value

        if not connection_dict:
            from snowflake.cli.api.exceptions import MissingConfigurationError

            raise MissingConfigurationError(
                f"Connection {connection_name} is not configured"
            )

        return connection_dict

    def get_connection_dict(self, connection_name: str) -> dict:
        """
        Get connection configuration by name.

        Args:
            connection_name: Name of the connection

        Returns:
            Dictionary of connection parameters
        """
        result = self._get_connection_dict_internal(connection_name)
        return self._transform_private_key_raw(result)

    def _get_all_connections_dict(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all connection configurations as nested dictionary.

        Returns:
            Dictionary mapping connection names to their configurations
        """
        self._ensure_initialized()

        if not self._config_cache:
            assert self._resolver is not None
            self._config_cache = self._resolver.resolve()

        connections: Dict[str, Dict[str, Any]] = {}
        connections_prefix = "connections."

        for key, value in self._config_cache.items():
            if key.startswith(connections_prefix):
                # Parse "connections.{name}.{param}"
                parts = key[len(connections_prefix) :].split(".", 1)
                if len(parts) == 2:
                    conn_name, param_name = parts
                    if conn_name not in connections:
                        connections[conn_name] = {}

                    # Skip internal markers, but ensure connection exists
                    if param_name == "_empty_connection":
                        # This is just a marker for empty connections
                        # Connection dict already created above
                        continue

                    connections[conn_name][param_name] = value

        return connections

    def get_all_connections(self, include_env_connections: bool = False) -> dict:
        """
        Get all connection configurations.

        Args:
            include_env_connections: If True, include connections created from
                                    environment variables. Default False for
                                    backward compatibility with legacy behavior.

        Returns:
            Dictionary mapping connection names to ConnectionConfig objects
        """
        from snowflake.cli.api.config import ConnectionConfig

        if not include_env_connections:
            # Only return connections from file sources (matching legacy behavior)
            return self._get_file_based_connections()

        # Return all connections including environment-based ones
        connections_dict = self._get_all_connections_dict()
        return {
            name: ConnectionConfig.from_dict(config)
            for name, config in connections_dict.items()
        }

    def _get_file_based_connections(self) -> dict:
        """
        Get connections only from file sources.

        Excludes connections that exist solely due to environment variables
        or CLI parameters. Matches legacy behavior.

        Returns:
            Dictionary mapping connection names to ConnectionConfig objects
        """
        from snowflake.cli.api.config import ConnectionConfig

        self._ensure_initialized()

        # Only query file sources: SnowSQL config, CLI config.toml, connections.toml
        file_source_names = {"snowsql_config", "cli_config_toml", "connections_toml"}

        connections: Dict[str, Dict[str, Any]] = {}
        connections_prefix = "connections."

        assert self._resolver is not None
        for source in self._resolver._sources:  # noqa: SLF001
            if source.source_name not in file_source_names:
                continue

            try:
                source_values = source.discover()
                for key, config_value in source_values.items():
                    if key.startswith(connections_prefix):
                        parts = key[len(connections_prefix) :].split(".", 1)
                        if len(parts) == 2:
                            conn_name, param_name = parts
                            if conn_name not in connections:
                                connections[conn_name] = {}

                            # Skip internal markers
                            if param_name != "_empty_connection":
                                connections[conn_name][param_name] = config_value.value
            except Exception:
                # Silently skip sources that fail to discover
                pass

        return {
            name: ConnectionConfig.from_dict(config)
            for name, config in connections.items()
        }

    def invalidate_cache(self) -> None:
        """
        Invalidate the provider's cache, forcing it to re-read configuration on next access.

        This is useful when configuration files are modified externally.
        """
        self._initialized = False
        self._config_cache.clear()
        if hasattr(self, "_last_config_override"):
            self._last_config_override = None


def _is_alternative_config_enabled() -> bool:
    """
    Check if alternative configuration handling is enabled via environment variable.
    Does not use the built-in feature flags mechanism.
    """
    return os.environ.get(ALTERNATIVE_CONFIG_ENV_VAR, "").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def get_config_provider() -> ConfigProvider:
    """
    Factory function to get the appropriate configuration provider
    based on environment variable.
    """
    if _is_alternative_config_enabled():
        return AlternativeConfigProvider()
    return LegacyConfigProvider()


_config_provider_instance: Optional[ConfigProvider] = None


def get_config_provider_singleton() -> ConfigProvider:
    """
    Get or create singleton instance of configuration provider.
    """
    global _config_provider_instance
    if _config_provider_instance is None:
        _config_provider_instance = get_config_provider()
    return _config_provider_instance


def reset_config_provider():
    """
    Reset the config provider singleton.
    Useful for testing and when config source changes.
    """
    global _config_provider_instance
    _config_provider_instance = None
