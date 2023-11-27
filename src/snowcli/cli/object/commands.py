from __future__ import annotations

import typer
from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.constants import SUPPORTED_OBJECTS
from snowcli.cli.object.manager import ObjectManager
from snowcli.cli.object.stage.commands import app as stage_app
from snowcli.output.decorators import with_output
from snowcli.output.types import QueryResult

app = typer.Typer(
    name="object",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages Snowflake objects like warehouses and stages",
)
app.add_typer(stage_app)


NameArgument = typer.Argument(None, help="Name of the object")
ObjectArgument = typer.Argument(
    None,
    help="Type of object. For example table, procedure, streamlit.",
    case_sensitive=False,
)
LikeOption = typer.Option(
    "%%",
    "--like",
    "-l",
    help='Regular expression for filtering the functions by name. For example, `list --like "my%"` lists '
    "all functions in the **dev** (default) environment that begin with “my”.",
)

SUPPORTED_TYPES_MSG = "Supported types:\n\n" + ", ".join(SUPPORTED_OBJECTS)


@app.command(
    "list",
    help=f"Lists all available Snowflake objects of given type. {SUPPORTED_TYPES_MSG}",
)
@with_output
@global_options_with_connection
def list_(
    object_type: str = ObjectArgument,
    like: str = LikeOption,
    **options,
):
    return QueryResult(ObjectManager().show(object_type=object_type, like=like))


@app.command(
    help=f"Drops Snowflake object of given name and type. {SUPPORTED_TYPES_MSG}"
)
@with_output
@global_options_with_connection
def drop(object_type: str = ObjectArgument, object_name: str = NameArgument, **options):
    return QueryResult(ObjectManager().drop(object_type=object_type, name=object_name))


@app.command(
    help=f"Provides description of an object of given type. {SUPPORTED_TYPES_MSG}"
)
@with_output
@global_options_with_connection
def describe(
    object_type: str = ObjectArgument, object_name: str = NameArgument, **options
):
    return QueryResult(
        ObjectManager().describe(object_type=object_type, name=object_name)
    )
