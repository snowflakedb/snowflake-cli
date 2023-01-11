#!/usr/bin/env python
from __future__ import annotations

import typer
from snowcli import config
from snowcli.config import AppConfig
from snowcli.utils import print_db_cursor

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
EnvironmentOption = typer.Option("dev", help="Environment name")


@app.command("status")
def warehouse_status(environment: str = EnvironmentOption):
    """
    Show the status of each warehouse in the configured environment.
    """
    env_conf = AppConfig().config.get(environment)

    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.showWarehouses(
            database=env_conf.get("database"),
            schema=env_conf.get("schema"),
            role=env_conf.get("role"),
            warehouse=env_conf.get("warehouse"),
        )
        print_db_cursor(results, ["name", "state", "queued", "resumed_on"])
