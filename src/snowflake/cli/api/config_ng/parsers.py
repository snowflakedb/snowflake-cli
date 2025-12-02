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
from typing import Any, Dict, cast

import tomlkit


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

                if "connections" not in result:
                    result["connections"] = {}
                if conn_name not in result["connections"]:
                    result["connections"][conn_name] = {}

                for key, value in config[section].items():
                    mapped_key = cls.SNOWSQL_KEY_MAP.get(key, key)
                    result["connections"][conn_name][mapped_key] = value

            elif section == "variables":
                result["variables"] = dict(config[section])

        return cls._apply_default_connection_fallback(result)

    @classmethod
    def _apply_default_connection_fallback(
        cls, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Apply fallback of default connection parameters to other connections.

        SnowSQL treats values declared directly under [connections] as defaults that
        propagate to other named connections lacking those fields.
        """
        connections = config.get("connections")
        if not isinstance(connections, dict):
            return config

        default_params = connections.get("default")
        if not isinstance(default_params, dict) or not default_params:
            return config

        for conn_name, conn_params in connections.items():
            if conn_name == "default" or not isinstance(conn_params, dict):
                continue
            for key, value in default_params.items():
                conn_params.setdefault(key, value)

        return config


class TOMLParser:
    """Parse TOML format to nested dict."""

    @staticmethod
    def parse(content: str) -> Dict[str, Any]:
        """
        Parse TOML format from string.

        TOML is already nested, so this wraps tomlkit.loads().unwrap().
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
        return cast(Dict[str, Any], tomlkit.loads(content).unwrap())
