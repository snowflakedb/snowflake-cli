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
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Final, Optional

if TYPE_CHECKING:
    from snowflake.cli.api.config_ng.resolver import ConfigurationResolver
    from snowflake.cli.api.config_ng.source_manager import SourceManager

ALTERNATIVE_CONFIG_ENV_VAR: Final[str] = "SNOWFLAKE_CLI_CONFIG_V2_ENABLED"


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
            return get_config_section("connections", connection_name)
        except KeyError:
            from snowflake.cli.api.exceptions import MissingConfigurationError

            raise MissingConfigurationError(
                f"Connection {connection_name} is not configured"
            )

    def get_all_connections(self, include_env_connections: bool = False) -> dict:
        from snowflake.cli.api.config import ConnectionConfig, get_config_section

        # Legacy provider ignores the flag since it never had env connections
        connections = get_config_section("connections")
        return {
            name: ConnectionConfig.from_dict(config)
            for name, config in connections.items()
        }


class AlternativeConfigProvider(ConfigProvider):
    """
    New configuration provider using config_ng resolution system.

    This provider uses ConfigurationResolver to discover values from:
    - CLI arguments (highest priority)
    - Environment variables (SNOWFLAKE_* and SNOWSQL_*)
    - Configuration files (SnowCLI TOML and SnowSQL config)

    Maintains backward compatibility with LegacyConfigProvider output format.
    """

    def __init__(
        self,
        source_manager: Optional["SourceManager"] = None,
        cli_context_getter: Optional[Any] = None,
    ) -> None:
        """
        Initialize provider with optional dependencies for testing.

        Args:
            source_manager: Optional source manager (for testing)
            cli_context_getter: Optional CLI context getter function (for testing)
        """
        self._source_manager = source_manager
        self._cli_context_getter = (
            cli_context_getter or self._default_cli_context_getter
        )
        self._resolver: Optional["ConfigurationResolver"] = None
        self._config_cache: Dict[str, Any] = {}
        self._initialized: bool = False
        self._last_config_override: Optional[Path] = None

    @staticmethod
    def _default_cli_context_getter():
        """Default implementation that accesses global CLI context."""
        from snowflake.cli.api.cli_global_context import get_cli_context

        return get_cli_context()

    def _ensure_initialized(self) -> None:
        """Lazily initialize the resolver on first use."""
        # Check if config_file_override has changed
        try:
            cli_context = self._cli_context_getter()
            current_override = cli_context.config_file_override

            # If override changed, force re-initialization
            if current_override != self._last_config_override:
                self._initialized = False
                self._config_cache.clear()
                self._last_config_override = current_override
        except Exception:
            pass

        if self._initialized:
            return

        from snowflake.cli.api.config_ng import ConfigurationResolver
        from snowflake.cli.api.config_ng.source_factory import create_default_sources
        from snowflake.cli.api.config_ng.source_manager import SourceManager

        # Get CLI context
        try:
            cli_context = self._cli_context_getter()
            cli_context_dict = cli_context.connection_context.present_values_as_dict()
        except Exception:
            cli_context_dict = {}

        # Create or use provided source manager
        if self._source_manager is None:
            sources = create_default_sources(cli_context_dict)
            self._source_manager = SourceManager(sources)

        # Create resolver
        self._resolver = ConfigurationResolver(
            sources=self._source_manager.get_sources()
        )

        # Initialize cache (resolver returns nested dict)
        if not self._config_cache:
            self._config_cache = self._resolver.resolve()

        # Record telemetry about config sources used
        self._record_config_telemetry()

        self._initialized = True

    def _record_config_telemetry(self) -> None:
        """Record configuration source usage to telemetry system."""
        if self._resolver is None:
            return

        try:
            from snowflake.cli.api.config_ng.telemetry_integration import (
                record_config_source_usage,
            )

            record_config_source_usage(self._resolver)
        except Exception:
            # Don't break initialization if telemetry fails
            pass

    def read_config(self) -> None:
        """
        Load configuration from all sources.
        Resolver returns nested dict structure.
        """
        self._initialized = False
        self._config_cache.clear()
        self._last_config_override = None
        self._ensure_initialized()

        # Resolver returns nested dict
        assert self._resolver is not None
        self._config_cache = self._resolver.resolve()

    def get_section(self, *path) -> dict:
        """
        Navigate nested dict to get configuration section.

        Args:
            *path: Section path components (e.g., "connections", "prod")

        Returns:
            Dictionary of section contents

        Example:
            Cache: {"connections": {"prod": {"account": "val"}}}
            get_section("connections", "prod") -> {"account": "val"}
        """
        self._ensure_initialized()

        if not path:
            return self._config_cache

        # Navigate nested structure
        result = self._config_cache
        for part in path:
            if not isinstance(result, dict) or part not in result:
                return {}
            result = result[part]

        return result if isinstance(result, dict) else {}

    def get_value(self, *path, key: str, default: Optional[Any] = None) -> Any:
        """
        Get single configuration value by navigating nested dict.

        Args:
            *path: Path to section
            key: Configuration key
            default: Default value if not found

        Returns:
            Configuration value or default
        """
        self._ensure_initialized()

        # Navigate to section, then get key
        section = self.get_section(*path)
        return section.get(key, default)

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
        Check if configuration section exists by navigating nested dict.

        Args:
            *path: Section path

        Returns:
            True if section exists
        """
        self._ensure_initialized()

        if not path:
            return True

        # Navigate nested structure
        result = self._config_cache
        for part in path:
            if not isinstance(result, dict) or part not in result:
                return False
            result = result[part]

        return True

    def _get_connection_dict_internal(self, connection_name: str) -> Dict[str, Any]:
        """
        Get connection configuration by navigating nested dict.

        Note: The resolver already merged general params into each connection
        during the OVERLAY phase, so we just return the connection dict directly.

        Args:
            connection_name: Name of the connection

        Returns:
            Dictionary of connection parameters
        """
        from snowflake.cli.api.exceptions import MissingConfigurationError

        self._ensure_initialized()

        # Get connection from nested dict
        connections = self._config_cache.get("connections", {})
        if connection_name in connections and isinstance(
            connections[connection_name], dict
        ):
            result = connections[connection_name]
            # Allow empty connections - they're valid (just have no parameters set)
            return result

        raise MissingConfigurationError(
            f"Connection {connection_name} is not configured"
        )

    def get_connection_dict(self, connection_name: str) -> dict:
        """
        Get connection configuration by name.

        Args:
            connection_name: Name of the connection

        Returns:
            Dictionary of connection parameters
        """
        return self._get_connection_dict_internal(connection_name)

    def _get_all_connections_dict(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all connections from nested dict.

        Returns:
            Dictionary mapping connection names to their configurations
        """
        self._ensure_initialized()

        connections = self._config_cache.get("connections", {})
        return connections if isinstance(connections, dict) else {}

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
        from snowflake.cli.api.config_ng.constants import FILE_SOURCE_NAMES

        self._ensure_initialized()

        connections: Dict[str, Dict[str, Any]] = {}

        assert self._resolver is not None
        for source in self._resolver.get_sources():
            if source.source_name not in FILE_SOURCE_NAMES:
                continue

            try:
                source_data = source.discover()  # Returns nested dict
                if "connections" in source_data:
                    for conn_name, conn_config in source_data["connections"].items():
                        if isinstance(conn_config, dict):
                            connections[conn_name] = conn_config
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


def is_alternative_config_enabled() -> bool:
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
    if is_alternative_config_enabled():
        return AlternativeConfigProvider()
    return LegacyConfigProvider()


_CONFIG_PROVIDER_LOCK: Final = threading.Lock()
_config_provider_instance: Optional[ConfigProvider] = None


def get_config_provider_singleton() -> ConfigProvider:
    """
    Get or create singleton instance of configuration provider.
    """
    global _config_provider_instance
    if _config_provider_instance is None:
        with _CONFIG_PROVIDER_LOCK:
            if _config_provider_instance is None:
                _config_provider_instance = get_config_provider()
    return _config_provider_instance


def reset_config_provider():
    """
    Reset the config provider singleton.
    Useful for testing and when config source changes.
    """
    global _config_provider_instance
    with _CONFIG_PROVIDER_LOCK:
        _config_provider_instance = None
