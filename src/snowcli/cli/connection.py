#!/usr/bin/env python
from __future__ import annotations

import typer
from rich import print
from rich.table import Table
from snowcli.config import AppConfig
from snowcli.snowsql_config import SnowsqlConfig

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})


@app.command()
def list():
    """
    List Snowflake connections.
    """
    app_cfg = AppConfig().config
    # if key 'snowsql_config_path' isn't defined in app_cfg
    if "snowsql_config_path" not in app_cfg:
        # set snowsql_config_path to ~/.snowsql/config
        app_cfg["snowsql_config_path"] = "~/.snowsql/config"
        print("No snowsql config path set. Using default...")

    print(f"Using {app_cfg['snowsql_config_path']}...")

    cfg = SnowsqlConfig(app_cfg["snowsql_config_path"])
    table = Table("Connection", "Account", "Username")
    for (connection_name, v) in cfg.config.items():
        if connection_name.startswith("connections."):
            connection_name = connection_name.replace("connections.", "")
            if "account" in v:
                table.add_row(connection_name, v["account"], v["username"])
            if "accountname" in v:
                table.add_row(connection_name, v["accountname"], v["username"])
    print(table)


@app.command()
def add(
    connection: str = typer.Option(
        ...,
        prompt="Name for this connection",
        help="Snowflake connection name",
    ),
    account: str = typer.Option(
        ...,
        prompt="Snowflake account",
        help="Snowflake account name",
    ),
    username: str = typer.Option(
        ...,
        prompt="Snowflake username",
        help="Snowflake username",
    ),
    password: str = typer.Option(
        ...,
        prompt="Snowflake password",
        help="Snowflake password",
        hide_input=True,
    ),
):
    app_cfg = AppConfig().config
    if "snowsql_config_path" not in app_cfg:
        cfg = SnowsqlConfig()
    else:
        cfg = SnowsqlConfig(app_cfg["snowsql_config_path"])
    connection_entry = {
        "account": account,
        "username": username,
        "password": password,
    }
    cfg.add_connection(connection, connection_entry)
    print(f"Wrote new connection {connection} to {cfg.path}")
