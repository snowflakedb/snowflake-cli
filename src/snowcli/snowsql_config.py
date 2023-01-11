from __future__ import annotations

import configparser
import os


class SnowsqlConfig:
    def __init__(self, path="~/.snowsql/config"):
        self.path = path
        self.config = configparser.ConfigParser(inline_comment_prefixes="#")
        self.config.read(os.path.expanduser(path))

    def get_connection(self, connection_name):
        connection = self.config["connections." + connection_name]
        # Remap names to appropriate args in Python Connector API
        connection = {
            k.replace("name", ""): v.strip('"') for k, v in connection.items()
        }
        return connection

    def add_connection(self, connection_name, entry):
        self.config[f"connections.{connection_name}"] = {}
        connection = self.config[f"connections.{connection_name}"]
        for (k, v) in entry.items():
            connection[k] = v

        with open(os.path.expanduser(self.path), "w+") as f:
            self.config.write(f)
