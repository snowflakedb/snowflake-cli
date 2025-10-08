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
File format handlers for configuration system.

This module implements handlers for:
- TOML configuration files (SnowCLI format)
- INI configuration files (SnowSQL format with key mapping)
"""

from __future__ import annotations

import configparser
from pathlib import Path
from types import MappingProxyType
from typing import Dict, List, Optional

import tomlkit
from snowflake.cli.api.config_ng.core import ConfigValue, SourcePriority
from snowflake.cli.api.config_ng.env_handlers import SNOWSQL_TO_SNOWCLI_KEY_MAPPINGS
from snowflake.cli.api.config_ng.handlers import SourceHandler

# Key mappings from SnowSQL to SnowCLI config keys (immutable)
SNOWSQL_CONFIG_KEY_MAPPINGS: MappingProxyType[str, str] = MappingProxyType(
    {
        **SNOWSQL_TO_SNOWCLI_KEY_MAPPINGS,  # Include env mappings (pwd → password)
        "accountname": "account",
        "username": "user",
        "dbname": "database",
        "databasename": "database",
        "schemaname": "schema",
        "warehousename": "warehouse",
        "rolename": "role",
    }
)


def get_snowsql_config_paths() -> List[Path]:
    """
    Get standard SnowSQL configuration file paths in FileSource precedence order.

    SnowSQL reads config files where "last one wins" (later files override earlier ones).
    Our FileSource uses "first one wins" (earlier files override later ones).

    This function returns paths in REVERSE order of SnowSQL's CNF_FILES to maintain
    compatibility with SnowSQL's precedence behavior.

    SnowSQL precedence (lowest to highest):
    1. Bundled default config
    2. System-wide configs (/etc/snowsql.cnf, /etc/snowflake/snowsql.cnf, /usr/local/etc/snowsql.cnf)
    3. User home config (~/.snowsql.cnf)
    4. User .snowsql directory config (~/.snowsql/config)
    5. RPM config (/usr/lib64/snowflake/snowsql/config) - if exists

    Returns:
        List of Path objects in FileSource precedence order (highest to lowest priority).
        Only includes paths that exist on the filesystem.
    """
    home_dir = Path.home()

    # Define paths in FileSource order (first = highest priority)
    # This is REVERSE of SnowSQL's order to maintain same effective precedence
    paths_to_check = [
        # Highest priority in both systems
        home_dir / ".snowsql" / "config",  # User .snowsql directory config
        home_dir / ".snowsql.cnf",  # User home config (legacy)
        Path("/usr/local/etc/snowsql.cnf"),  # Local system config
        Path("/etc/snowflake/snowsql.cnf"),  # Alternative system config
        Path("/etc/snowsql.cnf"),  # System-wide config
        # Bundled default config would go here but we typically don't ship one
        # Lowest priority in both systems
    ]

    # Check for RPM config (highest priority in SnowSQL if it exists)
    rpm_config = Path("/usr/lib64/snowflake/snowsql/config")
    if rpm_config.exists():
        paths_to_check.insert(0, rpm_config)  # Add as highest priority

    # Return only paths that exist
    return [p for p in paths_to_check if p.exists()]


class TomlFileHandler(SourceHandler):
    """
    Handler for TOML configuration files.
    Supports section navigation for nested configurations.

    Example:
        # Config file: ~/.snowflake/connections.toml
        [default]
        account = "my_account"
        user = "my_user"

        # With section_path=["default"]
        TomlFileHandler(section_path=["default"]).discover_from_file(path)
        # Returns: {"account": "my_account", "user": "my_user"}
    """

    def __init__(self, section_path: Optional[List[str]] = None):
        """
        Initialize with optional section path.

        Args:
            section_path: Path to section in TOML
                         Example: ["connections", "default"] for [connections.default]
                         None or [] means root level
        """
        self._section_path = section_path or []
        self._cached_data: Optional[Dict] = None
        self._cached_file: Optional[Path] = None

    @property
    def source_name(self) -> str:
        if self._section_path:
            section = ".".join(self._section_path)
            return f"toml:{section}"
        return "toml:root"

    @property
    def priority(self) -> SourcePriority:
        return SourcePriority.FILE

    @property
    def handler_type(self) -> str:
        return "toml"

    def can_handle(self) -> bool:
        """TOML handler is always available."""
        return True

    def can_handle_file(self, file_path: Path) -> bool:
        """Check if file is TOML format."""
        return file_path.suffix.lower() in (".toml", ".tml")

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """Not directly called - file handlers use discover_from_file."""
        raise NotImplementedError(
            "TomlFileHandler requires file_path. Use discover_from_file() instead."
        )

    def discover_from_file(
        self,
        file_path: Path,
        key: Optional[str] = None,
    ) -> Dict[str, ConfigValue]:
        """
        Discover values from TOML file.

        Args:
            file_path: Path to TOML file
            key: Specific key to discover, or None for all

        Returns:
            Dictionary of discovered values
        """
        # Load and cache file data
        if self._cached_file != file_path:
            try:
                with open(file_path) as f:
                    self._cached_data = tomlkit.load(f)
                    self._cached_file = file_path
            except (OSError, tomlkit.exceptions.TOMLKitError):
                # File doesn't exist or invalid TOML
                return {}

        # Navigate to section
        data = self._cached_data
        for section in self._section_path:
            if isinstance(data, dict) and section in data:
                data = data[section]
            else:
                return {}  # Section doesn't exist

        # Ensure data is a dictionary
        if not isinstance(data, dict):
            return {}

        # Extract values
        values = {}
        if key is not None:
            if key in data:
                raw = data[key]
                values[key] = ConfigValue(
                    key=key,
                    value=raw,  # TOML already parsed
                    source_name=self.source_name,
                    priority=self.priority,
                    raw_value=str(raw) if raw is not None else None,
                )
        else:
            for k, v in data.items():
                if isinstance(k, str):  # Only process string keys
                    values[k] = ConfigValue(
                        key=k,
                        value=v,
                        source_name=self.source_name,
                        priority=self.priority,
                        raw_value=str(v) if v is not None else None,
                    )

        return values

    def supports_key(self, key: str) -> bool:
        """TOML can handle any string key."""
        return isinstance(key, str)


class IniFileHandler(SourceHandler):
    """
    Handler for INI format configuration files.
    Supports SnowSQL-specific key naming and mappings.

    SnowSQL Multi-File Support:
        SnowSQL reads from multiple config file locations (system-wide, user home, etc.)
        where later files override earlier ones. To maintain this behavior with FileSource:

        1. Use get_snowsql_config_paths() to get paths in correct precedence order
        2. FileSource will process them with "first wins" logic, which matches
           SnowSQL's effective behavior due to reversed ordering

    Key Mappings (SnowSQL → SnowCLI):
    - accountname → account
    - username → user
    - dbname/databasename → database
    - schemaname → schema
    - warehousename → warehouse
    - rolename → role
    - pwd → password

    Example SnowSQL config (INI format):
        [connections.default]
        accountname = my_account
        username = my_user
        password = secret123

    Example usage with multiple files:
        from snowflake.cli.api.config_ng import (
            FileSource, IniFileHandler, get_snowsql_config_paths
        )

        snowsql_config_handler = IniFileHandler(source_name="snowsql_config")
        source = FileSource(
            file_paths=get_snowsql_config_paths(),
            handlers=[snowsql_config_handler]
        )
    """

    def __init__(
        self,
        section_path: Optional[List[str]] = None,
        source_name: str = "snowsql_config",
    ):
        """
        Initialize with optional section path and source name.

        Args:
            section_path: Path to section in config file
                         Default: ["connections"] for SnowSQL compatibility
            source_name: Name to identify this handler instance (e.g., "snowsql_config", "ini_config")
                        Default: "snowsql_config" for backward compatibility
        """
        self._section_path = section_path or ["connections"]
        self._source_name = source_name
        self._cached_data: Optional[configparser.ConfigParser] = None
        self._cached_file: Optional[Path] = None

    @property
    def source_name(self) -> str:
        return self._source_name

    @property
    def priority(self) -> SourcePriority:
        return SourcePriority.FILE

    @property
    def handler_type(self) -> str:
        return "ini"

    def can_handle(self) -> bool:
        """SnowSQL handler is always available."""
        return True

    def can_handle_file(self, file_path: Path) -> bool:
        """Check if file is SnowSQL config file."""
        # SnowSQL config is typically ~/.snowsql/config (no extension)
        # or ~/.snowsql.cnf, /etc/snowsql.cnf, etc.
        if file_path.parent.name == ".snowsql" and file_path.name == "config":
            return True
        if file_path.suffix.lower() == ".cnf":
            return True
        # For backward compatibility during migration, also handle .toml
        return file_path.suffix.lower() in (".toml", ".tml")

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """Not directly called - file handlers use discover_from_file."""
        raise NotImplementedError(
            "IniFileHandler requires file_path. Use discover_from_file() instead."
        )

    def discover_from_file(
        self,
        file_path: Path,
        key: Optional[str] = None,
    ) -> Dict[str, ConfigValue]:
        """
        Discover values from SnowSQL config with key mapping.

        Args:
            file_path: Path to SnowSQL config file (INI format)
            key: Specific key to discover (SnowCLI format), or None

        Returns:
            Dictionary with normalized SnowCLI keys
        """
        # Load and cache file data
        if self._cached_file != file_path:
            try:
                parser = configparser.ConfigParser()
                parser.read(file_path)
                self._cached_data = parser
                self._cached_file = file_path
            except (OSError, configparser.Error):
                return {}

        # Ensure we have cached data
        if self._cached_data is None:
            return {}

        # Build the section name from section_path
        # INI uses dot notation: connections.default becomes "connections.default"
        section_name = ".".join(self._section_path) if self._section_path else None

        # Get the data from the appropriate section
        data = {}
        if section_name:
            if self._cached_data.has_section(section_name):
                data = dict(self._cached_data.items(section_name))
            else:
                # Try to find subsections (e.g., if section_path is ["connections"])
                # Look for all sections starting with "connections."
                if len(self._section_path) == 1:
                    base_section = self._section_path[0]
                    if self._cached_data.has_section(base_section):
                        data = dict(self._cached_data.items(base_section))

        if not data:
            return {}

        # Extract and map keys
        values = {}

        if key is not None:
            # Reverse lookup: find SnowSQL key for CLI key
            snowsql_key = self._get_snowsql_key(key)
            # Check both original case and lowercase
            for k in [snowsql_key, snowsql_key.lower()]:
                if k in data:
                    raw = data[k]
                    values[key] = ConfigValue(
                        key=key,  # Normalized SnowCLI key
                        value=raw,
                        source_name=self.source_name,
                        priority=self.priority,
                        raw_value=f"{k}={raw}" if k != key else str(raw),
                    )
                    break
        else:
            for snowsql_key, value in data.items():
                if not isinstance(snowsql_key, str):
                    continue

                # Map to SnowCLI key (lowercase)
                snowsql_key_lower = snowsql_key.lower()
                cli_key = SNOWSQL_CONFIG_KEY_MAPPINGS.get(
                    snowsql_key_lower, snowsql_key_lower
                )

                values[cli_key] = ConfigValue(
                    key=cli_key,
                    value=value,
                    source_name=self.source_name,
                    priority=self.priority,
                    raw_value=(
                        f"{snowsql_key}={value}"
                        if snowsql_key_lower != cli_key
                        else str(value)
                    ),
                )

        return values

    def supports_key(self, key: str) -> bool:
        """Any string key can be represented in SnowSQL config."""
        return isinstance(key, str)

    def _get_snowsql_key(self, cli_key: str) -> str:
        """Reverse mapping: CLI key → SnowSQL key."""
        for snowsql_key, cli_mapped_key in SNOWSQL_CONFIG_KEY_MAPPINGS.items():
            if cli_mapped_key == cli_key:
                return snowsql_key
        return cli_key

    def get_cli_key(self, snowsql_key: str) -> str:
        """Forward mapping: SnowSQL key → CLI key."""
        snowsql_key_lower = snowsql_key.lower()
        return SNOWSQL_CONFIG_KEY_MAPPINGS.get(snowsql_key_lower, snowsql_key_lower)
