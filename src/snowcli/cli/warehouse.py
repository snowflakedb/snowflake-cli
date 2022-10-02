#!/usr/bin/env python
# -*- coding: utf-8 -*-

import typer

from snowcli import config, utils
from snowcli.config import AppConfig
from snowcli.snowsql_config import SnowsqlConfig
from snowcli.utils import print_db_cursor

app = typer.Typer()
EnvironmentOption = typer.Option("dev", help='Environment name')

@app.command("status")
def warehouse_status(environment: str = EnvironmentOption):
    """
    List streamlit apps.
    """
    env_conf = AppConfig().config.get(environment)

    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.showWarehouses(
            database=env_conf.get('database'),
            schema=env_conf.get('schema'),
            role=env_conf.get('role'),
            warehouse=env_conf.get('warehouse'))
        print_db_cursor(results, ['name', 'state', 'queued', 'resumed_on'])
