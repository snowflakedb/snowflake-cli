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
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Final, List, Optional

from snowflake.cli.api.config_ng.constants import SNOWFLAKE_HOME_ENV
from snowflake.cli.api.config_ng.core import SourceType, ValueSource

log = logging.getLogger(__name__)


class SnowSQLSection(Enum):
    """
    SnowSQL configuration file section names.

    These sections can be present in SnowSQL INI config files.
    """

    CONNECTIONS = "connections"
    VARIABLES = "variables"
    OPTIONS = "options"


class SnowSQLConfigFile(ValueSource):
    """
    SnowSQL configuration file source with two-phase design.

    Phase 1: Acquire content (read and merge multiple config files)
    Phase 2: Parse content (using SnowSQLParser)

    Reads multiple config files in order and MERGES them (SnowSQL behavior).
    Later files override earlier files for the same keys.
    Returns configuration for ALL connections.

    Config files searched (in order):
    1. /etc/snowsql.cnf (system-wide)
    2. /etc/snowflake/snowsql.cnf (alternative system)
    3. /usr/local/etc/snowsql.cnf (local system)
    4. ~/.snowsql.cnf (legacy user config)
    5. ~/.snowsql/config (current user config)
    """

    def __init__(
        self, content: Optional[str] = None, config_paths: Optional[List[Path]] = None
    ):
        """
        Initialize SnowSQL config file source.

        Args:
            content: Optional string content for testing (bypasses file I/O)
            config_paths: Optional custom config file paths
        """
        self._content = content
        self._config_paths = config_paths or self._get_default_paths()

    @staticmethod
    def _get_default_paths() -> List[Path]:
        """Get standard SnowSQL config file paths."""
        snowflake_home = os.environ.get(SNOWFLAKE_HOME_ENV)
        if snowflake_home:
            snowflake_home_path = Path(snowflake_home).expanduser()
            if snowflake_home_path.exists():
                return [snowflake_home_path / "config"]

        return [
            Path("/etc/snowsql.cnf"),
            Path("/etc/snowflake/snowsql.cnf"),
            Path("/usr/local/etc/snowsql.cnf"),
            Path.home() / ".snowsql.cnf",
            Path.home() / ".snowsql" / "config",
        ]

    @classmethod
    def from_string(cls, content: str) -> "SnowSQLConfigFile":
        """
        Create source from string content (for testing).

        Args:
            content: INI format configuration as string

        Returns:
            SnowSQLConfigFile instance using string content
        """
        return cls(content=content)

    @property
    def source_name(self) -> "ValueSource.SourceName":
        return "snowsql_config"

    @property
    def source_type(self) -> SourceType:
        return SourceType.FILE

    def discover(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Two-phase discovery: acquire content → parse.

        Phase 1: Get content (from string or by reading and merging files)
        Phase 2: Parse content using SnowSQLParser

        Returns:
            Nested dict structure: {"connections": {...}, "variables": {...}}
        """
        from snowflake.cli.api.config_ng.parsers import SnowSQLParser

        # Phase 1: Content acquisition
        if self._content is not None:
            content = self._content
        else:
            content = self._read_and_merge_files()

        # Phase 2: Parse content
        return SnowSQLParser.parse(content)

    def _read_and_merge_files(self) -> str:
        """
        Read all config files and merge into single INI string.

        Returns:
            Merged INI content as string
        """
        merged_config = configparser.ConfigParser()

        for config_file in self._config_paths:
            if config_file.exists():
                try:
                    merged_config.read(config_file)
                except Exception as e:
                    log.debug("Failed to read SnowSQL config %s: %s", config_file, e)

        # Convert merged config to string
        from io import StringIO

        output = StringIO()
        merged_config.write(output)
        return output.getvalue()

    def supports_key(self, key: str) -> bool:
        return key in self.discover()


class CliConfigFile(ValueSource):
    """
    CLI config.toml file source with two-phase design.

    Phase 1: Acquire content (find and read first config file)
    Phase 2: Parse content (using TOMLParser)

    Scans for config.toml files in order and uses FIRST file found (CLI behavior).
    Does NOT merge multiple files - first found wins.
    Returns configuration for ALL connections.

    Search order (when no override is set):
    1. ./config.toml (current directory)
    2. ~/.snowflake/config.toml (user config)
    """

    def __init__(
        self, content: Optional[str] = None, search_paths: Optional[List[Path]] = None
    ):
        """
        Initialize CLI config file source.

        Args:
            content: Optional string content for testing (bypasses file I/O)
            search_paths: Optional custom search paths
        """
        self._content = content
        self._search_paths = search_paths or self._get_default_paths()

    @staticmethod
    def _get_default_paths() -> List[Path]:
        """Get standard CLI config search paths."""
        # Check for config file override from CLI context first
        try:
            from snowflake.cli.api.cli_global_context import get_cli_context

            cli_context = get_cli_context()
            config_override = cli_context.config_file_override
            if config_override:
                return [Path(config_override)]
        except Exception:
            log.debug("CLI context not available, using standard config paths")

        # Use SNOWFLAKE_HOME if set and directory exists
        snowflake_home = os.environ.get(SNOWFLAKE_HOME_ENV)
        if snowflake_home:
            snowflake_home_path = Path(snowflake_home).expanduser()
            if snowflake_home_path.exists():
                return [snowflake_home_path / "config.toml"]

        # Standard paths
        return [
            Path.cwd() / "config.toml",
            Path.home() / ".snowflake" / "config.toml",
        ]

    @classmethod
    def from_string(cls, content: str) -> "CliConfigFile":
        """
        Create source from TOML string (for testing).

        Args:
            content: TOML format configuration as string

        Returns:
            CliConfigFile instance using string content
        """
        return cls(content=content)

    @property
    def source_name(self) -> "ValueSource.SourceName":
        return "cli_config_toml"

    @property
    def source_type(self) -> SourceType:
        return SourceType.FILE

    def discover(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Two-phase discovery: acquire content → parse.

        Phase 1: Get content (from string or by reading first existing file)
        Phase 2: Parse content using TOMLParser

        Returns:
            Nested dict structure with all TOML sections preserved
        """
        from snowflake.cli.api.config_ng.parsers import TOMLParser

        # Phase 1: Content acquisition
        if self._content is not None:
            content = self._content
        else:
            content = self._read_first_file()

        if not content:
            return {}

        # Phase 2: Parse content
        return TOMLParser.parse(content)

    def _read_first_file(self) -> str:
        """
        Read first existing config file.

        Returns:
            File content as string, or empty string if no file found
        """
        for config_file in self._search_paths:
            if config_file.exists():
                try:
                    return config_file.read_text()
                except Exception as e:
                    log.debug("Failed to read CLI config %s: %s", config_file, e)

        return ""

    def supports_key(self, key: str) -> bool:
        return key in self.discover()


class ConnectionsConfigFile(ValueSource):
    """
    Dedicated connections.toml file source with three-phase design.

    Phase 1: Acquire content (read file)
    Phase 2: Parse content (using TOMLParser)
    Phase 3: Normalize legacy format (connections.toml specific)

    Reads ~/.snowflake/connections.toml specifically.
    Returns configuration for ALL connections.

    Supports both legacy formats:
    1. Direct connection sections (legacy):
       [default]
       database = "value"

    2. Nested under [connections] section:
       [connections.default]
       database = "value"

    Both are normalized to nested format: {"connections": {"default": {...}}}
    """

    def __init__(self, content: Optional[str] = None, file_path: Optional[Path] = None):
        """
        Initialize connections.toml source.

        Args:
            content: Optional string content for testing (bypasses file I/O)
            file_path: Optional custom file path
        """
        self._content = content
        self._file_path = file_path or self._get_default_path()

    @staticmethod
    def _get_default_path() -> Path:
        """Get standard connections.toml path."""
        snowflake_home = os.environ.get(SNOWFLAKE_HOME_ENV)
        if snowflake_home:
            snowflake_home_path = Path(snowflake_home).expanduser()
            if snowflake_home_path.exists():
                return snowflake_home_path / "connections.toml"
        return Path.home() / ".snowflake" / "connections.toml"

    @classmethod
    def from_string(cls, content: str) -> "ConnectionsConfigFile":
        """
        Create source from TOML string (for testing).

        Args:
            content: TOML format configuration as string

        Returns:
            ConnectionsConfigFile instance using string content
        """
        return cls(content=content)

    @property
    def source_name(self) -> "ValueSource.SourceName":
        return "connections_toml"

    @property
    def source_type(self) -> SourceType:
        return SourceType.FILE

    @property
    def is_connections_file(self) -> bool:
        """Mark this as the dedicated connections file source."""
        return True

    def get_defined_connections(self) -> set[str]:
        """
        Return set of connection names that are defined in connections.toml.
        This is used by the resolver to implement replacement behavior.
        """
        try:
            data = self.discover()
            connections_section = data.get("connections", {})
            if isinstance(connections_section, dict):
                return set(connections_section.keys())
            return set()
        except Exception as e:
            log.debug("Failed to get defined connections: %s", e)
            return set()

    def discover(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Three-phase discovery: acquire content → parse → normalize.

        Phase 1: Get content (from string or file)
        Phase 2: Parse TOML (generic parser)
        Phase 3: Normalize legacy format (connections.toml specific)

        Returns:
            Nested dict structure: {"connections": {"conn_name": {...}}}
        """
        from snowflake.cli.api.config_ng.parsers import TOMLParser

        # Phase 1: Content acquisition
        if self._content is not None:
            content = self._content
        else:
            if not self._file_path.exists():
                return {}
            try:
                content = self._file_path.read_text()
            except Exception as e:
                log.debug("Failed to read connections.toml: %s", e)
                return {}

        # Phase 2: Parse TOML (generic parser)
        try:
            data = TOMLParser.parse(content)
        except Exception as e:
            log.debug("Failed to parse connections.toml: %s", e)
            return {}

        # Phase 3: Normalize legacy format (connections.toml specific)
        return self._normalize_connections_format(data)

    @staticmethod
    def _normalize_connections_format(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize connections.toml format to standard structure.

        Supports:
        - Legacy: [connection_name] → {"connections": {"connection_name": {...}}}
        - New: [connections.connection_name] → {"connections": {"connection_name": {...}}}

        Args:
            data: Parsed TOML data

        Returns:
            Normalized structure with connections under "connections" key
        """
        result: Dict[str, Any] = {}

        # Handle direct connection sections (legacy format)
        # Any top-level section that's not "connections" is treated as a connection
        for section_name, section_data in data.items():
            if isinstance(section_data, dict) and section_name != "connections":
                if "connections" not in result:
                    result["connections"] = {}
                result["connections"][section_name] = section_data

        # Handle nested [connections] section (new format)
        connections_section = data.get("connections", {})
        if isinstance(connections_section, dict) and connections_section:
            if "connections" not in result:
                result["connections"] = {}
            # Merge with any legacy connections found above
            # (nested format takes precedence if there's overlap)
            result["connections"].update(connections_section)

        return result

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
    def source_name(self) -> "ValueSource.SourceName":
        return "snowsql_env"

    @property
    def source_type(self) -> SourceType:
        return SourceType.OVERLAY

    def discover(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Discover SNOWSQL_* environment variables.
        Returns flat values at root level (no connection prefix).
        """
        result: Dict[str, Any] = {}

        for env_var, config_key in self.ENV_VAR_MAPPING.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Only set if not already set by a previous env var
                # (e.g., SNOWSQL_ACCOUNT takes precedence over SNOWSQL_ACCOUNTNAME)
                if config_key not in result:
                    result[config_key] = env_value

        return result

    def supports_key(self, key: str) -> bool:
        # Check if any env var for this key is set
        for env_var, config_key in self.ENV_VAR_MAPPING.items():
            if config_key == key and os.getenv(env_var) is not None:
                return True
        return False


# Base configuration keys that can be set via environment
_ENV_CONFIG_KEYS: Final[list[str]] = [
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
    def source_name(self) -> "ValueSource.SourceName":
        return "connection_specific_env"

    @property
    def source_type(self) -> SourceType:
        return SourceType.OVERLAY

    def discover(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Discover SNOWFLAKE_CONNECTIONS_* environment variables.
        Returns nested dict structure.

        Pattern: SNOWFLAKE_CONNECTIONS_<NAME>_<KEY>=value
                 -> {"connections": {"{name}": {"{key}": value}}}
        """
        result: Dict[str, Any] = {}

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

                # Build nested structure
                if "connections" not in result:
                    result["connections"] = {}
                if conn_name not in result["connections"]:
                    result["connections"][conn_name] = {}

                result["connections"][conn_name][config_key] = env_value

        return result

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
    def source_name(self) -> "ValueSource.SourceName":
        return "cli_env"

    @property
    def source_type(self) -> SourceType:
        return SourceType.OVERLAY

    def discover(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Discover general SNOWFLAKE_* environment variables.
        Returns flat values at root level.

        Pattern: SNOWFLAKE_<KEY>=value -> {key: value}
        """
        result: Dict[str, Any] = {}

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
                result[config_key] = env_value

        return result

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
    def source_name(self) -> "ValueSource.SourceName":
        return "cli_arguments"

    @property
    def source_type(self) -> SourceType:
        return SourceType.OVERLAY

    def discover(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract non-None values from CLI context.
        CLI arguments are already parsed by the framework.
        Returns flat values at root level.
        """
        result: Dict[str, Any] = {}

        for k, v in self._cli_context.items():
            # Skip None values (not provided on CLI)
            if v is None:
                continue

            result[k] = v

        return result

    def supports_key(self, key: str) -> bool:
        """Check if key is present in CLI context with non-None value."""
        return key in self._cli_context and self._cli_context[key] is not None


def get_merged_variables(cli_variables: Optional[List[str]] = None) -> Dict[str, str]:
    """
    Merge SnowSQL [variables] with CLI -D parameters.

    Precedence: SnowSQL variables (lower) < -D parameters (higher)

    Args:
        cli_variables: List of "key=value" strings from -D parameters

    Returns:
        Dictionary of merged variables (key -> value)
    """
    from snowflake.cli.api.config_provider import get_config_provider_singleton

    # Start with SnowSQL variables from config
    provider = get_config_provider_singleton()
    try:
        snowsql_vars = provider.get_section(SnowSQLSection.VARIABLES.value)
    except Exception:
        # If variables section doesn't exist or provider not initialized, start with empty dict
        snowsql_vars = {}

    # Parse and overlay -D parameters (higher precedence)
    if cli_variables:
        from snowflake.cli.api.commands.utils import parse_key_value_variables

        cli_vars_parsed = parse_key_value_variables(cli_variables)
        for var in cli_vars_parsed:
            snowsql_vars[var.key] = var.value

    return snowsql_vars
