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
- CLI environment variables (SNOWFLAKE_* patterns)
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
    Returns configuration for ALL connections.

    Config files searched (in order, when not in test mode):
    1. Bundled default config (if in package)
    2. /etc/snowsql.cnf (system-wide)
    3. /etc/snowflake/snowsql.cnf (alternative system)
    4. /usr/local/etc/snowsql.cnf (local system)
    5. ~/.snowsql.cnf (legacy user config)
    6. ~/.snowsql/config (current user config)

    In test mode (when config_file_override is set), SnowSQL config files are skipped
    to ensure test isolation.
    """

    # SnowSQL uses different key names - map them to CLI standard names
    KEY_MAPPING = {
        "accountname": "account",
        "username": "user",
        "rolename": "role",
        "warehousename": "warehouse",
        "schemaname": "schema",
        "dbname": "database",
        "pwd": "password",
        # Keys that don't need mapping (already correct)
        "password": "password",
        "database": "database",
        "schema": "schema",
        "role": "role",
        "warehouse": "warehouse",
        "host": "host",
        "port": "port",
        "protocol": "protocol",
        "authenticator": "authenticator",
        "private_key_path": "private_key_path",
        "private_key_passphrase": "private_key_passphrase",
    }

    def __init__(self):
        """Initialize SnowSQL config file source."""
        # Use SNOWFLAKE_HOME if set and directory exists, otherwise use standard paths
        snowflake_home = os.environ.get("SNOWFLAKE_HOME")
        if snowflake_home:
            snowflake_home_path = Path(snowflake_home).expanduser()
            if snowflake_home_path.exists():
                # Use only the SnowSQL config file within SNOWFLAKE_HOME
                self._config_files = [snowflake_home_path / "config"]
            else:
                # SNOWFLAKE_HOME set but doesn't exist, use standard paths
                self._config_files = [
                    Path("/etc/snowsql.cnf"),
                    Path("/etc/snowflake/snowsql.cnf"),
                    Path("/usr/local/etc/snowsql.cnf"),
                    Path.home() / ".snowsql.cnf",
                    Path.home() / ".snowsql" / "config",
                ]
        else:
            # Standard paths when SNOWFLAKE_HOME not set
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
        Returns keys in format: connections.{name}.{param} for ALL connections.
        """
        merged_values: Dict[str, ConfigValue] = {}

        for config_file in self._config_files:
            if not config_file.exists():
                continue

            try:
                config = configparser.ConfigParser()
                config.read(config_file)

                # Process all connection sections
                for section in config.sections():
                    if section.startswith("connections"):
                        # Extract connection name
                        if section == "connections":
                            # This is default connection
                            connection_name = "default"
                        else:
                            # Format: connections.qa6 -> qa6
                            connection_name = (
                                section.split(".", 1)[1]
                                if "." in section
                                else "default"
                            )

                        section_data = dict(config[section])

                        # Add all params for this connection
                        for param_key, param_value in section_data.items():
                            # Map SnowSQL key names to CLI standard names
                            normalized_key = self.KEY_MAPPING.get(param_key, param_key)
                            full_key = f"connections.{connection_name}.{normalized_key}"
                            if key is None or full_key == key:
                                merged_values[full_key] = ConfigValue(
                                    key=full_key,
                                    value=param_value,
                                    source_name=self.source_name,
                                    raw_value=f"{param_key}={param_value}",  # Show original key in raw_value
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
    Returns configuration for ALL connections.

    Search order (when no override is set):
    1. ./config.toml (current directory)
    2. ~/.snowflake/config.toml (user config)

    When config_file_override is set (e.g., in tests), only that file is used.
    """

    def __init__(self):
        """Initialize CLI config file source."""
        # Check for config file override from CLI context first
        try:
            from snowflake.cli.api.cli_global_context import get_cli_context

            cli_context = get_cli_context()
            config_override = cli_context.config_file_override
            if config_override:
                self._search_paths = [Path(config_override)]
                return
        except Exception:
            pass

        # Use SNOWFLAKE_HOME if set and directory exists, otherwise use standard paths
        snowflake_home = os.environ.get("SNOWFLAKE_HOME")
        if snowflake_home:
            snowflake_home_path = Path(snowflake_home).expanduser()
            if snowflake_home_path.exists():
                # Use only config.toml within SNOWFLAKE_HOME
                self._search_paths = [snowflake_home_path / "config.toml"]
            else:
                # SNOWFLAKE_HOME set but doesn't exist, use standard paths
                self._search_paths = [
                    Path.cwd() / "config.toml",
                    Path.home() / ".snowflake" / "config.toml",
                ]
        else:
            # Standard paths when SNOWFLAKE_HOME not set
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
        Returns keys in format: connections.{name}.{param} for ALL connections.
        """
        for config_file in self._search_paths:
            if config_file.exists():
                return self._parse_toml_file(config_file, key)

        return {}

    def _parse_toml_file(
        self, file_path: Path, key: Optional[str] = None
    ) -> Dict[str, ConfigValue]:
        """Parse TOML file and extract ALL connection configurations."""
        try:
            with open(file_path, "rb") as f:
                data = tomllib.load(f)

            result = {}

            # Get all connections
            connections = data.get("connections", {})
            for conn_name, conn_data in connections.items():
                if isinstance(conn_data, dict):
                    # Process parameters if they exist
                    for param_key, param_value in conn_data.items():
                        full_key = f"connections.{conn_name}.{param_key}"
                        if key is None or full_key == key:
                            result[full_key] = ConfigValue(
                                key=full_key,
                                value=param_value,
                                source_name=self.source_name,
                                raw_value=param_value,
                            )

                    # For empty connections, we need to ensure they are recognized
                    # even if they have no parameters. We add a special marker.
                    if not conn_data:  # Empty connection section
                        marker_key = f"connections.{conn_name}._empty_connection"
                        if key is None or marker_key == key:
                            result[marker_key] = ConfigValue(
                                key=marker_key,
                                value=True,
                                source_name=self.source_name,
                                raw_value=True,
                            )

            return result

        except Exception as e:
            log.debug("Failed to parse CLI config %s: %s", file_path, e)
            return {}

    def supports_key(self, key: str) -> bool:
        return key in self.discover()


class ConnectionsConfigFile(ValueSource):
    """
    Dedicated connections.toml file source.

    Reads ~/.snowflake/connections.toml specifically.
    Returns configuration for ALL connections.
    """

    def __init__(self):
        """Initialize connections.toml source."""
        # Use SNOWFLAKE_HOME if set and directory exists, otherwise use standard path
        snowflake_home = os.environ.get("SNOWFLAKE_HOME")
        if snowflake_home:
            snowflake_home_path = Path(snowflake_home).expanduser()
            if snowflake_home_path.exists():
                self._file_path = snowflake_home_path / "connections.toml"
            else:
                self._file_path = Path.home() / ".snowflake" / "connections.toml"
        else:
            self._file_path = Path.home() / ".snowflake" / "connections.toml"

    @property
    def source_name(self) -> str:
        return "connections_toml"

    @property
    def is_connections_file(self) -> bool:
        """Mark this as the dedicated connections file source."""
        return True

    def get_defined_connections(self) -> set[str]:
        """
        Return set of connection names that are defined in connections.toml.
        This is used by the resolver to implement replacement behavior.
        """
        if not self._file_path.exists():
            return set()

        try:
            with open(self._file_path, "rb") as f:
                data = tomllib.load(f)

            connection_names = set()

            # Check for direct connection sections (legacy format)
            for section_name, section_data in data.items():
                if isinstance(section_data, dict) and section_name != "connections":
                    connection_names.add(section_name)

            # Check for nested [connections] section format
            connections_section = data.get("connections", {})
            if isinstance(connections_section, dict):
                for conn_name in connections_section.keys():
                    connection_names.add(conn_name)

            return connection_names

        except Exception as e:
            log.debug("Failed to read connections.toml: %s", e)
            return set()

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Read connections.toml if it exists.
        Returns keys in format: connections.{name}.{param} for ALL connections.

        Supports both legacy formats:
        1. Direct connection sections (legacy):
           [default]
           database = "value"

        2. Nested under [connections] section:
           [connections.default]
           database = "value"
        """
        if not self._file_path.exists():
            return {}

        try:
            with open(self._file_path, "rb") as f:
                data = tomllib.load(f)

            result = {}

            # Check for direct connection sections (legacy format)
            for section_name, section_data in data.items():
                if isinstance(section_data, dict) and section_name != "connections":
                    # This is a direct connection section like [default]
                    for param_key, param_value in section_data.items():
                        full_key = f"connections.{section_name}.{param_key}"
                        if key is None or full_key == key:
                            result[full_key] = ConfigValue(
                                key=full_key,
                                value=param_value,
                                source_name=self.source_name,
                                raw_value=param_value,
                            )

            # Check for nested [connections] section format
            connections_section = data.get("connections", {})
            if isinstance(connections_section, dict):
                for conn_name, conn_data in connections_section.items():
                    if isinstance(conn_data, dict):
                        for param_key, param_value in conn_data.items():
                            full_key = f"connections.{conn_name}.{param_key}"
                            if key is None or full_key == key:
                                result[full_key] = ConfigValue(
                                    key=full_key,
                                    value=param_value,
                                    source_name=self.source_name,
                                    raw_value=param_value,
                                )

            return result

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


# Base configuration keys that can be set via environment
_ENV_CONFIG_KEYS = [
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
    "workload_identity_provider",
    "private_key_file",
    "private_key_path",  # Used by integration tests
    "private_key_raw",  # Used by integration tests
    "private_key_passphrase",  # Private key passphrase for encrypted keys
    "token",  # OAuth token
    "session_token",  # Session token for session-based authentication
    "master_token",  # Master token for advanced authentication
    "token_file_path",
    "oauth_client_id",
    "oauth_client_secret",
    "oauth_authorization_url",
    "oauth_token_request_url",
    "oauth_redirect_uri",
    "oauth_scope",
    "oauth_enable_pkce",  # Fixed typo: was "oatuh_enable_pkce"
    "oauth_enable_refresh_tokens",
    "oauth_enable_single_use_refresh_tokens",
    "client_store_temporary_credential",
]


class ConnectionSpecificEnvironment(ValueSource):
    """
    Connection-specific environment variables source.

    Discovers SNOWFLAKE_CONNECTIONS_<name>_<key> environment variables.
    Returns prefixed keys: connections.{name}.{key}

    Examples:
        SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT=x -> connections.integration.account=x
        SNOWFLAKE_CONNECTIONS_DEV_USER=y -> connections.dev.user=y
    """

    @property
    def source_name(self) -> str:
        return "connection_specific_env"

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Discover SNOWFLAKE_CONNECTIONS_* environment variables.
        Returns connection-specific (prefixed) keys only.

        Pattern: SNOWFLAKE_CONNECTIONS_<NAME>_<KEY>=value -> connections.{name}.{key}=value
        """
        values: Dict[str, ConfigValue] = {}

        # Scan all environment variables
        for env_name, env_value in os.environ.items():
            # Check for connection-specific pattern: SNOWFLAKE_CONNECTIONS_<NAME>_<KEY>
            if env_name.startswith("SNOWFLAKE_CONNECTIONS_"):
                # Extract remainder after the prefix
                remainder = env_name[len("SNOWFLAKE_CONNECTIONS_") :]

                # Find the longest matching key suffix from known config keys to
                # correctly handle underscores both in connection names and keys
                match: tuple[str, str] | None = None
                for candidate in sorted(_ENV_CONFIG_KEYS, key=len, reverse=True):
                    key_suffix = "_" + candidate.upper()
                    if remainder.endswith(key_suffix):
                        conn_name_upper = remainder[: -len(key_suffix)]
                        if conn_name_upper:  # ensure non-empty connection name
                            match = (conn_name_upper, candidate)
                            break

                if not match:
                    # Unknown/unsupported key suffix; ignore
                    continue

                conn_name_upper, config_key = match
                conn_name = conn_name_upper.lower()

                full_key = f"connections.{conn_name}.{config_key}"
                if key is None or full_key == key:
                    values[full_key] = ConfigValue(
                        key=full_key,
                        value=env_value,
                        source_name=self.source_name,
                        raw_value=f"{env_name}={env_value}",
                    )

        return values

    def supports_key(self, key: str) -> bool:
        # Check if key matches pattern connections.{name}.{param}
        if key.startswith("connections."):
            parts = key.split(".", 2)
            if len(parts) == 3:
                _, conn_name, config_key = parts
                env_var = (
                    f"SNOWFLAKE_CONNECTIONS_{conn_name.upper()}_{config_key.upper()}"
                )
                return os.getenv(env_var) is not None
        return False


class CliEnvironment(ValueSource):
    """
    CLI general environment variables source.

    Discovers general SNOWFLAKE_* environment variables (not connection-specific).
    Returns flat keys that apply to all connections.

    Examples:
        SNOWFLAKE_ACCOUNT -> account (general, applies to all connections)
        SNOWFLAKE_USER -> user
        SNOWFLAKE_PASSWORD -> password
    """

    @property
    def source_name(self) -> str:
        return "cli_env"

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Discover general SNOWFLAKE_* environment variables.
        Returns general (flat) keys only.

        Pattern: SNOWFLAKE_<KEY>=value -> {key}=value
        """
        values: Dict[str, ConfigValue] = {}

        # Scan all environment variables
        for env_name, env_value in os.environ.items():
            if not env_name.startswith("SNOWFLAKE_"):
                continue

            # Skip connection-specific variables
            if env_name.startswith("SNOWFLAKE_CONNECTIONS_"):
                continue

            # Check for general pattern: SNOWFLAKE_<KEY>
            config_key_upper = env_name[len("SNOWFLAKE_") :]
            config_key = config_key_upper.lower()

            if config_key in _ENV_CONFIG_KEYS:
                if key is None or config_key == key:
                    values[config_key] = ConfigValue(
                        key=config_key,
                        value=env_value,
                        source_name=self.source_name,
                        raw_value=f"{env_name}={env_value}",
                    )

        return values

    def supports_key(self, key: str) -> bool:
        # Only support flat keys (not prefixed with connections.)
        if "." in key:
            return False

        # Check if the general env var exists
        env_var = f"SNOWFLAKE_{key.upper()}"
        return os.getenv(env_var) is not None


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
