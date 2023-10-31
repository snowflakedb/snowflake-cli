from __future__ import annotations

import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.object.show.manager import ObjectManager
from snowcli.cli.object.utils import ObjectType
from snowcli.output.decorators import with_output
from snowcli.output.types import QueryResult

app = typer.Typer(
    name="show",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Lists existing objects of given type",
)

LikeOption = typer.Option(
    "%%",
    "--like",
    "-l",
    help='Regular expression for filtering the functions by name. For example, `list --like "my%"` lists all functions in the **dev** (default) environment that begin with “my”.',
)


@app.command("warehouses")
@with_output
@global_options_with_connection
def warehouse_status(like: str, **options):
    """
    Shows existing warehouses
    """
    cursor = ObjectManager().show(ObjectType.WAREHOUSE, like)
    return QueryResult(cursor)
