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
Environment variable handlers for configuration system.

This module implements handlers for:
- SNOWFLAKE_* environment variables (SnowCLI format)
- SNOWSQL_* environment variables (Legacy SnowSQL format with key mapping)
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from snowflake.cli.api.config_ng.core import ConfigValue, SourcePriority
from snowflake.cli.api.config_ng.handlers import SourceHandler


class SnowCliEnvHandler(SourceHandler):
    """
    Handler for Snowflake CLI environment variables.
    Format: SNOWFLAKE_<KEY> → key
    Example: SNOWFLAKE_ACCOUNT → account
    """

    PREFIX = "SNOWFLAKE_"

    @property
    def source_name(self) -> str:
        return "snowflake_cli_env"

    @property
    def priority(self) -> SourcePriority:
        return SourcePriority.ENVIRONMENT

    @property
    def handler_type(self) -> str:
        return "snowflake_cli_env"

    def can_handle(self) -> bool:
        """Check if any SNOWFLAKE_* env vars are set."""
        return any(k.startswith(self.PREFIX) for k in os.environ)

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """Discover values from SNOWFLAKE_* environment variables."""
        values = {}

        if key is not None:
            # Discover specific key
            env_key = f"{self.PREFIX}{key.upper()}"
            if env_key in os.environ:
                raw = os.environ[env_key]
                values[key] = ConfigValue(
                    key=key,
                    value=self._parse_value(raw),
                    source_name=self.source_name,
                    priority=self.priority,
                    raw_value=raw,
                )
        else:
            # Discover all SNOWFLAKE_* variables
            for env_key, env_value in os.environ.items():
                if env_key.startswith(self.PREFIX):
                    config_key = env_key[len(self.PREFIX) :].lower()
                    values[config_key] = ConfigValue(
                        key=config_key,
                        value=self._parse_value(env_value),
                        source_name=self.source_name,
                        priority=self.priority,
                        raw_value=env_value,
                    )

        return values

    def supports_key(self, key: str) -> bool:
        """Any string key can be represented as SNOWFLAKE_* env var."""
        return isinstance(key, str)

    def _parse_value(self, value: str) -> Any:
        """
        Parse string value to appropriate type.
        Supports: boolean, integer, string
        """
        # Boolean - case-insensitive
        lower_val = value.lower()
        if lower_val in ("true", "1", "yes", "on"):
            return True
        if lower_val in ("false", "0", "no", "off"):
            return False

        # Integer
        try:
            return int(value)
        except ValueError:
            pass

        # String (default)
        return value


class SnowSqlEnvHandler(SourceHandler):
    """
    Handler for SnowSQL-compatible environment variables.
    Format: SNOWSQL_<KEY> → key
    Supports key mappings for SnowSQL-specific naming.

    Key Mappings (SnowSQL → SnowCLI):
    - PWD → password
    - All other keys map directly (ACCOUNT → account, USER → user, etc.)
    """

    PREFIX = "SNOWSQL_"

    # Key mappings from SnowSQL to SnowCLI
    # SnowSQL uses PWD, but SnowCLI uses PASSWORD
    KEY_MAPPINGS: Dict[str, str] = {
        "pwd": "password",
    }

    @property
    def source_name(self) -> str:
        return "snowsql_env"

    @property
    def priority(self) -> SourcePriority:
        return SourcePriority.ENVIRONMENT

    @property
    def handler_type(self) -> str:
        return "snowsql_env"

    def can_handle(self) -> bool:
        """Check if any SNOWSQL_* env vars are set."""
        return any(k.startswith(self.PREFIX) for k in os.environ)

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        """
        Discover values from SNOWSQL_* environment variables.
        Applies key mappings for compatibility.
        """
        values = {}

        if key is not None:
            # Reverse lookup: find SnowSQL key for CLI key
            snowsql_key = self.get_snowsql_key(key)
            env_key = f"{self.PREFIX}{snowsql_key.upper()}"

            if env_key in os.environ:
                raw = os.environ[env_key]
                values[key] = ConfigValue(
                    key=key,  # Normalized SnowCLI key
                    value=self._parse_value(raw),
                    source_name=self.source_name,
                    priority=self.priority,
                    raw_value=raw,
                )
        else:
            # Discover all SNOWSQL_* variables
            for env_key, env_value in os.environ.items():
                if env_key.startswith(self.PREFIX):
                    snowsql_key = env_key[len(self.PREFIX) :].lower()
                    # Map to SnowCLI key
                    config_key = self.KEY_MAPPINGS.get(snowsql_key, snowsql_key)

                    values[config_key] = ConfigValue(
                        key=config_key,
                        value=self._parse_value(env_value),
                        source_name=self.source_name,
                        priority=self.priority,
                        raw_value=env_value,
                    )

        return values

    def supports_key(self, key: str) -> bool:
        """Any string key can be represented as SNOWSQL_* env var."""
        return isinstance(key, str)

    def get_snowsql_key(self, cli_key: str) -> str:
        """Reverse mapping: CLI key → SnowSQL key."""
        for snowsql_key, cli_mapped_key in self.KEY_MAPPINGS.items():
            if cli_mapped_key == cli_key:
                return snowsql_key
        return cli_key

    def _parse_value(self, value: str) -> Any:
        """
        Parse string value to appropriate type.
        Supports: boolean, integer, string
        """
        # Boolean - case-insensitive
        lower_val = value.lower()
        if lower_val in ("true", "1", "yes", "on"):
            return True
        if lower_val in ("false", "0", "no", "off"):
            return False

        # Integer
        try:
            return int(value)
        except ValueError:
            pass

        # String (default)
        return value
