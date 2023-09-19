from __future__ import annotations

import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.warehouse.manager import WarehouseManager
from snowcli.output.decorators import with_output
from snowcli.output.types import QueryResult

app = typer.Typer(
    name="warehouse",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages warehouses.",
)


@app.command("status")
@with_output
@global_options_with_connection
def warehouse_status(**options):
    """
    Shows the status of each warehouse in the configured environment.
    """
    cursor = WarehouseManager().show()
    return QueryResult(cursor)
