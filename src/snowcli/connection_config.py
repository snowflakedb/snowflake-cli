from __future__ import annotations

import os
from configparser import ConfigParser
from pathlib import Path
from snowcli.exception import EnvironmentVariableNotFoundError


class ConnectionConfigs:
    def __init__(self, snowsql_config_path="~/.snowsql/config"):
        self.snowsql_config_path = snowsql_config_path
        self._config = ConfigParser(inline_comment_prefixes="#", interpolation=None)
        p = Path(snowsql_config_path).absolute().expanduser()
        self._config.read(p)

    def is_connection_exists(self, connection_name: str):
        return self._config.sections().__contains__(f"connections.{connection_name}")

    def get_connections(self):
        connections_names = [
            n for n in self._config.sections() if n.startswith("connections.")
        ]
        return {n: self._get_connection(n) for n in connections_names}

    def get_connection(self, connection_name: str):
        return self._get_connection(f"connections.{connection_name}")

    def add_connection(self, connection_name: str, entry: dict):
        self._config[f"connections.{connection_name}"] = {}
        connection = self._config[f"connections.{connection_name}"]
        for (k, v) in entry.items():
            connection[k] = v

        with open(os.path.expanduser(self.snowsql_config_path), "w+") as f:
            self._config.write(f)

    def _get_connection(self, connection_full_name: str):
        connection = self._config[connection_full_name]
        # Remap names to appropriate args in Python Connector API
        connection = {
            k.replace("name", ""): v.strip('"') for k, v in connection.items()
        }
        return self._replace_with_env_variables(connection)

    def _replace_with_env_variables(self, connection_sections: dict):
        for (name, value) in connection_sections.items():
            if value.startswith("$SNOWCLI_"):
                try:
                    connection_sections[name] = os.environ[value[1:]]
                except (KeyError):
                    raise EnvironmentVariableNotFoundError(value[1:]) from None
        return connection_sections
