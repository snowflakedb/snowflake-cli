from __future__ import annotations

import os
from configparser import ConfigParser
from pathlib import Path

from snowcli.exception import EnvironmentVariableNotFoundError


def get_absolute_path(str_path: str) -> Path:
    return Path(str_path).expanduser().absolute()


class ConnectionConfigs:
    def __init__(self, snowsql_config_path="~/.snowsql/config"):
        self.snowsql_config_path = snowsql_config_path
        self._config = ConfigParser(inline_comment_prefixes="#", interpolation=None)
        p = get_absolute_path(snowsql_config_path)
        self._config.read(p)

    def connection_exists(self, connection_name: str):
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
        for k, v in entry.items():
            connection[k] = v

        with open(os.path.expanduser(self.snowsql_config_path), "w+") as f:
            self._config.write(f)

    def _get_connection(self, connection_full_name: str):
        connection_parameters = self._config[connection_full_name]
        # Remap names to appropriate args in Python Connector API
        connection_parameters = {
            k.replace("name", ""): v.strip('"')
            for k, v in connection_parameters.items()
        }
        return self._replace_with_env_variables(connection_parameters)

    def _replace_with_env_variables(self, connection_parameters: dict):
        for name, value in connection_parameters.items():
            if value.startswith("$SNOWCLI_"):
                try:
                    connection_parameters[name] = os.environ[value[1:]]
                except KeyError:
                    raise EnvironmentVariableNotFoundError(value[1:])
        return connection_parameters
