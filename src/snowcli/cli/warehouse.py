from __future__ import annotations

import typer
from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.output.decorators import with_output
from snowcli.snow_connector import SqlExecutionMixin

app = typer.Typer(
    name="warehouse",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage warehouses",
)


class WarehouseManager(SqlExecutionMixin):
    def show(self) -> SnowflakeCursor:
        return self._execute_query("show warehouses")


@app.command("status")
@with_output
def warehouse_status(connection_name: str = ConnectionOption):
    """
    Show the status of each warehouse in the configured environment.
    """
    return WarehouseManager.from_connection(connection_name=connection_name).show()
