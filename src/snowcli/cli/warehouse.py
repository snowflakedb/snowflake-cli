from __future__ import annotations

import typer

from snowcli import config
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.config import connect_to_snowflake
from snowcli.output.printing import print_db_cursor

app = typer.Typer(
    name="warehouse",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage warehouses",
)
EnvironmentOption = typer.Option(None, help="Environment name")


@app.command("status")
def warehouse_status(environment: str = ConnectionOption):
    """
    Show the status of each warehouse in the configured environment.
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = conn.show_warehouses(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
        )
        print_db_cursor(results, ["name", "state", "queued", "resumed_on"])
