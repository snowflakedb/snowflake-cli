from __future__ import annotations

import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.warehouse.manager import WarehouseManager
from snowcli.output.decorators import with_output
from snowcli.output.printing import OutputData

app = typer.Typer(
    name="warehouse",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage warehouses",
)


@app.command("status")
@with_output
@global_options_with_connection
def warehouse_status(**options):
    """
    Show the status of each warehouse in the configured environment.
    """
    cursor = WarehouseManager().show()
    return OutputData.from_cursor(cursor)
