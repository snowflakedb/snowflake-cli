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
from typing import Any, Optional

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
    def get_all_connections(self) -> dict:
        """Get all connection configurations."""
        ...


class LegacyConfigProvider(ConfigProvider):
    """
    Current TOML-based configuration provider.
    Wraps existing implementation for compatibility.
    """

    def get_section(self, *path) -> dict:
        from snowflake.cli.api.config import get_config_section_internal

        return get_config_section_internal(*path)

    def get_value(self, *path, key: str, default: Optional[Any] = None) -> Any:
        from snowflake.cli.api.config import Empty, get_config_value_internal

        return get_config_value_internal(
            *path, key=key, default=default if default is not None else Empty
        )

    def set_value(self, path: list[str], value: Any) -> None:
        from snowflake.cli.api.config import set_config_value_internal

        set_config_value_internal(path, value)

    def unset_value(self, path: list[str]) -> None:
        from snowflake.cli.api.config import unset_config_value_internal

        unset_config_value_internal(path)

    def section_exists(self, *path) -> bool:
        from snowflake.cli.api.config import config_section_exists_internal

        return config_section_exists_internal(*path)

    def read_config(self) -> None:
        from snowflake.cli.api.config import _read_config_file

        _read_config_file()

    def get_connection_dict(self, connection_name: str) -> dict:
        from snowflake.cli.api.config import get_connection_dict_internal

        return get_connection_dict_internal(connection_name)

    def get_all_connections(self) -> dict:
        from snowflake.cli.api.config import get_all_connections_internal

        return get_all_connections_internal()


class AlternativeConfigProvider(ConfigProvider):
    """
    New configuration provider implementation.
    To be implemented with new logic while maintaining same output format.
    """

    def __init__(self):
        pass

    def get_section(self, *path) -> dict:
        raise NotImplementedError("Alternative config provider not yet implemented")

    def get_value(self, *path, key: str, default: Optional[Any] = None) -> Any:
        raise NotImplementedError("Alternative config provider not yet implemented")

    def set_value(self, path: list[str], value: Any) -> None:
        raise NotImplementedError("Alternative config provider not yet implemented")

    def unset_value(self, path: list[str]) -> None:
        raise NotImplementedError("Alternative config provider not yet implemented")

    def section_exists(self, *path) -> bool:
        raise NotImplementedError("Alternative config provider not yet implemented")

    def read_config(self) -> None:
        raise NotImplementedError("Alternative config provider not yet implemented")

    def get_connection_dict(self, connection_name: str) -> dict:
        raise NotImplementedError("Alternative config provider not yet implemented")

    def get_all_connections(self) -> dict:
        raise NotImplementedError("Alternative config provider not yet implemented")


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
