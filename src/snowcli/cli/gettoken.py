#!/usr/bin/env python
# -*- coding: utf-8 -*-

import typer

from snowcli import config, utils
from snowcli.config import AppConfig
from snowcli.snowsql_config import SnowsqlConfig
from snowcli.utils import print_db_cursor

app = typer.Typer()
EnvironmentOption = typer.Option("dev", help='Environment name')

@app.command("session")
def get_sess_token(environment: str = EnvironmentOption):
    """
    get a Token that can be used with DockerCli, GitCli
    """
    if config.isAuth():
        config.connectToSnowflake()
        print(config.snowflake_connection.ctx._rest._token)
