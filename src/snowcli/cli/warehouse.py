from __future__ import annotations

from functools import wraps

import typer
from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.snow_connector import SqlExecutionMixin
from snowcli.output.printing import print_db_cursor

app = typer.Typer(
    name="warehouse",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage warehouses",
)


class WarehouseManager(SqlExecutionMixin):
    def show(self) -> SnowflakeCursor:
        return self._execute_query("show warehouses")


def with_output(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        print_db_cursor(result)

    return wrapper


@app.command("status")
@with_output
def warehouse_status(connection_name: str = ConnectionOption):
    """
    Show the status of each warehouse in the configured environment.
    """
    results = WarehouseManager.from_connection(connection_name=connection_name).show()
    return results
