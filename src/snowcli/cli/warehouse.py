#!/usr/bin/env python
from __future__ import annotations

import typer

from snowcli import config
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.config import AppConfig
from snowcli.output.printing import print_db_cursor

app = typer.Typer(
    name="warehouse",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage warehouses",
)
EnvironmentOption = typer.Option("dev", help="Environment name")


@app.command("status")
def warehouse_status(environment: str = EnvironmentOption):
    """
    Show the status of each warehouse in the configured environment.
    """
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        results = config.snowflake_connection.show_warehouses(
            database=env_conf.get("database"),
            schema=env_conf.get("schema"),
            role=env_conf.get("role"),
            warehouse=env_conf.get("warehouse"),
        )
        print_db_cursor(results, ["name", "state", "queued", "resumed_on"])
