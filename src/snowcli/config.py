from __future__ import annotations

from pathlib import Path

import click
import toml
from snowcli.snow_connector import SnowflakeConnector
from snowcli.snowsql_config import SnowsqlConfig

snowflake_connection: SnowflakeConnector


class AppConfig:
    def __init__(self):
        self.path = self._find_app_toml()
        if self.path:
            self.config = toml.load(self.path)
        else:
            self.path = Path.cwd().joinpath("app.toml")
            self.config = {}

    def _find_app_toml(self):
        # Find first app.toml by traversing parent dirs up to home dir
        p = Path.cwd()
        while not any(p.glob("app.toml")) and p != p.home():
            p = p.parent

        if p == p.home():
            return None
        else:
            return next(p.glob("app.toml"))

    def save(self):
        with open(self.path, "w") as f:
            toml.dump(self.config, f)


def connectToSnowflake():
    global snowflake_connection
    cfg = AppConfig()
    snowsql_config = SnowsqlConfig(path=cfg.config.get("snowsql_config_path"))
    snowflake_connection = SnowflakeConnector(
        snowsql_config,
        cfg.config.get("snowsql_connection_name"),
    )


def isAuth():
    cfg = AppConfig()
    if "snowsql_config_path" not in cfg.config:
        click.echo("You must login first with `snow login`")
        return False
    return True
