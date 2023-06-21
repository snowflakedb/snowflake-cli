from __future__ import annotations

import typer
from rich import print
from rich.table import Table

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.config import AppConfig
from snowcli.connection_config import ConnectionConfigs

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage connection to Snowflake",
)


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

    connection_configs = ConnectionConfigs(app_cfg["snowsql_config_path"])
    table = Table("Connection", "Account", "Username")
    for (connection_name, v) in connection_configs.get_connections().items():
        connection_name = connection_name.replace("connections.", "")
        table.add_row(connection_name, v["account"], v["user"])
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
        connection_configs = ConnectionConfigs()
    else:
        connection_configs = ConnectionConfigs(app_cfg["snowsql_config_path"])
    connection_entry = {
        "account": account,
        "username": username,
        "password": password,
    }
    connection_configs.add_connection(connection, connection_entry)
    print(
        f"Wrote new connection {connection} to {connection_configs.snowsql_config_path}"
    )
