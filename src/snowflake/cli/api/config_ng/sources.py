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
Configuration sources for the Snowflake CLI.

This module implements concrete configuration sources that discover values from:
- SnowSQL configuration files (INI format, merged from multiple locations)
- CLI configuration files (TOML format, first-found)
- Connections configuration files (dedicated connections.toml)
- SnowSQL environment variables (SNOWSQL_* prefix)
- CLI environment variables (SNOWFLAKE_* and SNOWFLAKE_CONNECTION_* patterns)
- CLI command-line parameters

Precedence is determined by the order sources are provided to the resolver.
"""

from __future__ import annotations

import configparser
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from snowflake.cli.api.config_ng.core import ConfigValue, ValueSource

log = logging.getLogger(__name__)

# Try to import tomllib (Python 3.11+) or fall back to tomli
try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore


class SnowSQLConfigFile(ValueSource):
    """
    SnowSQL configuration file source.

    Reads multiple config files in order and MERGES them (SnowSQL behavior).
    Later files override earlier files for the same keys.

    Config files searched (in order):
    1. Bundled default config (if in package)
    2. /etc/snowsql.cnf (system-wide)
    3. /etc/snowflake/snowsql.cnf (alternative system)
    4. /usr/local/etc/snowsql.cnf (local system)
    5. ~/.snowsql.cnf (legacy user config)
    6. ~/.snowsql/config (current user config)
    """

    def __init__(self, connection_name: str = "default"):
        """
        Initialize SnowSQL config file source.

        Args:
            connection_name: Name of the connection to read from
        """
        self._connection_name = connection_name
        self._config_files = [
            Path("/etc/snowsql.cnf"),
            Path("/etc/snowflake/snowsql.cnf"),
            Path("/usr/local/etc/snowsql.cnf"),
            Path.home() / ".snowsql.cnf",
            Path.home() / ".snowsql" / "config",
        ]

    @property
    def source_name(self) -> str:
        return "snowsql_config"

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Read and MERGE all SnowSQL config files.
        Later files override earlier files (SnowSQL merging behavior).
        """
        merged_values: Dict[str, ConfigValue] = {}

        for config_file in self._config_files:
            if not config_file.exists():
                continue

            try:
                config = configparser.ConfigParser()
                config.read(config_file)

                # Try connection-specific section first: [connections.prod]
                section_name = f"connections.{self._connection_name}"
                if config.has_section(section_name):
                    section_data = dict(config[section_name])
                # Fall back to default [connections] section
                elif config.has_section("connections"):
                    section_data = dict(config["connections"])
                else:
                    continue

                # Merge values (later file wins for conflicts)
                for k, v in section_data.items():
                    if key is None or k == key:
                        merged_values[k] = ConfigValue(
                            key=k,
                            value=v,
                            source_name=self.source_name,
                            raw_value=v,
                        )

            except Exception as e:
                log.debug("Failed to read SnowSQL config %s: %s", config_file, e)

        return merged_values

    def supports_key(self, key: str) -> bool:
        return key in self.discover()


class CliConfigFile(ValueSource):
    """
    CLI config.toml file source.

    Scans for config.toml files in order and uses FIRST file found (CLI behavior).
    Does NOT merge multiple files - first found wins.

    Search order:
    1. ./config.toml (current directory)
    2. ~/.snowflake/config.toml (user config)
    """

    def __init__(self, connection_name: str = "default"):
        """
        Initialize CLI config file source.

        Args:
            connection_name: Name of the connection to read from
        """
        self._connection_name = connection_name
        self._search_paths = [
            Path.cwd() / "config.toml",
            Path.home() / ".snowflake" / "config.toml",
        ]

    @property
    def source_name(self) -> str:
        return "cli_config_toml"

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Find FIRST existing config file and use it (CLI behavior).
        Does NOT merge multiple files.
        """
        for config_file in self._search_paths:
            if config_file.exists():
                return self._parse_toml_file(config_file, key)

        return {}

    def _parse_toml_file(
        self, file_path: Path, key: Optional[str] = None
    ) -> Dict[str, ConfigValue]:
        """Parse TOML file and extract connection configuration."""
        try:
            with open(file_path, "rb") as f:
                data = tomllib.load(f)

            # Navigate to connections.<name>
            conn_data = data.get("connections", {}).get(self._connection_name, {})

            return {
                k: ConfigValue(
                    key=k, value=v, source_name=self.source_name, raw_value=v
                )
                for k, v in conn_data.items()
                if key is None or k == key
            }

        except Exception as e:
            log.debug("Failed to parse CLI config %s: %s", file_path, e)
            return {}

    def supports_key(self, key: str) -> bool:
        return key in self.discover()


class ConnectionsConfigFile(ValueSource):
    """
    Dedicated connections.toml file source.

    Reads ~/.snowflake/connections.toml specifically.
    """

    def __init__(self, connection_name: str = "default"):
        """
        Initialize connections.toml source.

        Args:
            connection_name: Name of the connection to read from
        """
        self._connection_name = connection_name
        self._file_path = Path.home() / ".snowflake" / "connections.toml"

    @property
    def source_name(self) -> str:
        return "connections_toml"

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """Read connections.toml if it exists."""
        if not self._file_path.exists():
            return {}

        try:
            with open(self._file_path, "rb") as f:
                data = tomllib.load(f)

            conn_data = data.get("connections", {}).get(self._connection_name, {})

            return {
                k: ConfigValue(
                    key=k, value=v, source_name=self.source_name, raw_value=v
                )
                for k, v in conn_data.items()
                if key is None or k == key
            }

        except Exception as e:
            log.debug("Failed to read connections.toml: %s", e)
            return {}

    def supports_key(self, key: str) -> bool:
        return key in self.discover()


class SnowSQLEnvironment(ValueSource):
    """
    SnowSQL environment variables source.

    Discovers SNOWSQL_* environment variables only.
    Simple prefix mapping without connection-specific variants.

    Examples:
        SNOWSQL_ACCOUNT -> account
        SNOWSQL_USER -> user
        SNOWSQL_PWD -> password
    """

    # Mapping of SNOWSQL_* env vars to configuration keys
    ENV_VAR_MAPPING = {
        "SNOWSQL_ACCOUNT": "account",
        "SNOWSQL_ACCOUNTNAME": "account",  # Alternative
        "SNOWSQL_USER": "user",
        "SNOWSQL_USERNAME": "user",  # Alternative
        "SNOWSQL_PWD": "password",
        "SNOWSQL_PASSWORD": "password",  # Alternative
        "SNOWSQL_DATABASE": "database",
        "SNOWSQL_DBNAME": "database",  # Alternative
        "SNOWSQL_SCHEMA": "schema",
        "SNOWSQL_SCHEMANAME": "schema",  # Alternative
        "SNOWSQL_ROLE": "role",
        "SNOWSQL_ROLENAME": "role",  # Alternative
        "SNOWSQL_WAREHOUSE": "warehouse",
        "SNOWSQL_WAREHOUSENAME": "warehouse",  # Alternative
        "SNOWSQL_PROTOCOL": "protocol",
        "SNOWSQL_HOST": "host",
        "SNOWSQL_PORT": "port",
        "SNOWSQL_REGION": "region",
        "SNOWSQL_AUTHENTICATOR": "authenticator",
        "SNOWSQL_PRIVATE_KEY_PASSPHRASE": "private_key_passphrase",
    }

    @property
    def source_name(self) -> str:
        return "snowsql_env"

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Discover SNOWSQL_* environment variables.
        No connection-specific variables supported.
        """
        values: Dict[str, ConfigValue] = {}

        for env_var, config_key in self.ENV_VAR_MAPPING.items():
            if key is not None and config_key != key:
                continue

            env_value = os.getenv(env_var)
            if env_value is not None:
                # Only set if not already set by a previous env var
                # (e.g., SNOWSQL_ACCOUNT takes precedence over SNOWSQL_ACCOUNTNAME)
                if config_key not in values:
                    values[config_key] = ConfigValue(
                        key=config_key,
                        value=env_value,
                        source_name=self.source_name,
                        raw_value=env_value,
                    )

        return values

    def supports_key(self, key: str) -> bool:
        # Check if any env var for this key is set
        for env_var, config_key in self.ENV_VAR_MAPPING.items():
            if config_key == key and os.getenv(env_var) is not None:
                return True
        return False


class CliEnvironment(ValueSource):
    """
    CLI environment variables source.

    Discovers SNOWFLAKE_* environment variables with two patterns:
    1. General: SNOWFLAKE_ACCOUNT (applies to all connections)
    2. Connection-specific: SNOWFLAKE_CONNECTION_<name>_ACCOUNT (overrides general)

    Connection-specific variables take precedence within this source.

    Examples:
        SNOWFLAKE_ACCOUNT -> account (general)
        SNOWFLAKE_CONNECTION_PROD_ACCOUNT -> account (for "prod" connection)
        SNOWFLAKE_USER -> user
        SNOWFLAKE_CONNECTION_DEV_USER -> user (for "dev" connection)
    """

    # Base configuration keys that can be set via environment
    CONFIG_KEYS = [
        "account",
        "user",
        "password",
        "database",
        "schema",
        "role",
        "warehouse",
        "protocol",
        "host",
        "port",
        "region",
        "authenticator",
    ]

    def __init__(self, connection_name: Optional[str] = None):
        """
        Initialize CLI environment source.

        Args:
            connection_name: Optional connection name for connection-specific vars
        """
        self._connection_name = connection_name

    @property
    def source_name(self) -> str:
        if self._connection_name:
            return f"cli_env:{self._connection_name}"
        return "cli_env"

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Discover SNOWFLAKE_* environment variables.

        Supports two patterns:
        1. SNOWFLAKE_ACCOUNT (general)
        2. SNOWFLAKE_CONNECTION_<name>_ACCOUNT (connection-specific, higher priority)
        """
        values: Dict[str, ConfigValue] = {}

        # Pattern 1: General SNOWFLAKE_* variables
        for config_key in self.CONFIG_KEYS:
            if key is not None and config_key != key:
                continue

            env_var = f"SNOWFLAKE_{config_key.upper()}"
            env_value = os.getenv(env_var)

            if env_value is not None:
                values[config_key] = ConfigValue(
                    key=config_key,
                    value=env_value,
                    source_name=self.source_name,
                    raw_value=env_value,
                )

        # Pattern 2: Connection-specific SNOWFLAKE_CONNECTION_<name>_* variables
        # These override general variables
        if self._connection_name:
            conn_prefix = f"SNOWFLAKE_CONNECTION_{self._connection_name.upper()}_"

            for config_key in self.CONFIG_KEYS:
                if key is not None and config_key != key:
                    continue

                env_var = f"{conn_prefix}{config_key.upper()}"
                env_value = os.getenv(env_var)

                if env_value is not None:
                    # Override general variable
                    values[config_key] = ConfigValue(
                        key=config_key,
                        value=env_value,
                        source_name=self.source_name,
                        raw_value=env_value,
                    )

        return values

    def supports_key(self, key: str) -> bool:
        if key not in self.CONFIG_KEYS:
            return False

        # Check general var
        if os.getenv(f"SNOWFLAKE_{key.upper()}") is not None:
            return True

        # Check connection-specific var
        if self._connection_name:
            conn_var = (
                f"SNOWFLAKE_CONNECTION_{self._connection_name.upper()}_{key.upper()}"
            )
            if os.getenv(conn_var) is not None:
                return True

        return False


class CliParameters(ValueSource):
    """
    CLI command-line parameters source.

    Highest priority source that extracts values from parsed CLI arguments.
    Values are already parsed by Typer/Click framework.

    Examples:
        --account my_account -> account: "my_account"
        --user alice -> user: "alice"
        -a my_account -> account: "my_account"
    """

    def __init__(self, cli_context: Optional[Dict[str, Any]] = None):
        """
        Initialize CLI parameters source.

        Args:
            cli_context: Dictionary of CLI arguments (key -> value)
        """
        self._cli_context = cli_context or {}

    @property
    def source_name(self) -> str:
        return "cli_arguments"

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Extract non-None values from CLI context.
        CLI arguments are already parsed by the framework.
        """
        values: Dict[str, ConfigValue] = {}

        for k, v in self._cli_context.items():
            # Skip None values (not provided on CLI)
            if v is None:
                continue

            if key is None or k == key:
                values[k] = ConfigValue(
                    key=k,
                    value=v,
                    source_name=self.source_name,
                    raw_value=v,
                )

        return values

    def supports_key(self, key: str) -> bool:
        """Check if key is present in CLI context with non-None value."""
        return key in self._cli_context and self._cli_context[key] is not None
