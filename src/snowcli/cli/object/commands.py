from __future__ import annotations

import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.object.show.manager import ObjectManager
from snowcli.cli.object.utils import ObjectType
from snowcli.output.decorators import with_output
from snowcli.output.types import QueryResult

from snowcli.cli.object.stage.commands import app as stage_app

app = typer.Typer(
    name="object",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages Snowflake objects like warehouses and stages",
)
app.add_typer(stage_app)  # type: ignore

LikeOption = typer.Option(
    "%%",
    "--like",
    "-l",
    help='Regular expression for filtering the functions by name. For example, `list --like "my%"` lists all functions in the **dev** (default) environment that begin with “my”.',
)

@app.command()
@with_output
@global_options_with_connection
def show(
        object_type: ObjectType = typer.Argument(None, help="Type of object"),
        like: str = LikeOption,
        **options
         ):
    return QueryResult(ObjectManager.show(object_type,like))


@app.command("warehouses")
@with_output
@global_options_with_connection
def warehouse_status(like: str, **options):
    """
    Shows existing warehouses
    """
    cursor = ObjectManager().show(ObjectType.WAREHOUSE, like)
    return QueryResult(cursor)
