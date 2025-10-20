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

"""Configuration parsers - decouple parsing from file I/O."""

import configparser
from typing import Any, Dict

# Try to import tomllib (Python 3.11+) or fall back to tomli
try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore


class SnowSQLParser:
    """Parse SnowSQL INI format to nested dict."""

    # Mapping of SnowSQL key names to CLI standard names
    SNOWSQL_KEY_MAP: Dict[str, str] = {
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
        "account": "account",
        "user": "user",
    }

    @classmethod
    def parse(cls, content: str) -> Dict[str, Any]:
        """
        Parse SnowSQL INI format from string.

        Args:
            content: INI format configuration as string

        Returns:
            Nested dict: {"connections": {...}, "variables": {...}}

        Example:
            Input:
                [connections.dev]
                accountname = myaccount
                username = myuser

                [variables]
                stage = mystage

            Output:
                {
                    "connections": {
                        "dev": {"account": "myaccount", "user": "myuser"}
                    },
                    "variables": {"stage": "mystage"}
                }
        """
        config = configparser.ConfigParser()
        config.read_string(content)

        result: Dict[str, Any] = {}

        for section in config.sections():
            if section.startswith("connections"):
                # Extract connection name from section
                if section == "connections":
                    conn_name = "default"
                else:
                    conn_name = (
                        section.split(".", 1)[1] if "." in section else "default"
                    )

                # Ensure connections dict exists
                if "connections" not in result:
                    result["connections"] = {}
                if conn_name not in result["connections"]:
                    result["connections"][conn_name] = {}

                # Map keys and add to connection
                for key, value in config[section].items():
                    mapped_key = cls.SNOWSQL_KEY_MAP.get(key, key)
                    result["connections"][conn_name][mapped_key] = value

            elif section == "variables":
                # Process variables section
                result["variables"] = dict(config[section])

        return result


class TOMLParser:
    """Parse TOML format to nested dict."""

    @staticmethod
    def parse(content: str) -> Dict[str, Any]:
        """
        Parse TOML format from string.

        TOML is already nested, so this just wraps tomllib.loads().
        All TOML sources (CLI config, connections.toml) use this parser.

        Args:
            content: TOML format configuration as string

        Returns:
            Nested dict with TOML structure preserved

        Example:
            Input:
                [connections.prod]
                account = "myaccount"
                user = "myuser"

            Output:
                {"connections": {"prod": {"account": "myaccount", "user": "myuser"}}}
        """
        return tomllib.loads(content)
