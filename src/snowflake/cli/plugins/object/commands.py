from __future__ import annotations

from typing import Optional, Tuple

import typer
from click import ClickException
from snowflake.cli.api.commands.decorators import (
    global_options_with_connection,
    with_output,
)
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS
from snowflake.cli.api.constants import SUPPORTED_OBJECTS
from snowflake.cli.api.output.types import QueryResult
from snowflake.cli.api.project.util import is_valid_identifier
from snowflake.cli.plugins.object.manager import ObjectManager
from snowflake.cli.plugins.object.stage.commands import app as stage_app

app = typer.Typer(
    name="object",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages Snowflake objects like warehouses and stages",
)
app.add_typer(stage_app)


NameArgument = typer.Argument(help="Name of the object")
ObjectArgument = typer.Argument(
    help="Type of object. For example table, procedure, streamlit.",
    case_sensitive=False,
)
LikeOption = typer.Option(
    "%%",
    "--like",
    "-l",
    help='SQL LIKE pattern for filtering objects by name. For example, `list function --like "my%"` lists '
    "all functions that begin with “my”.",
)

# Scope names here must replace spaces with '-'. For example 'compute pool' is 'compute-pool'.
VALID_SCOPES = ["database", "schema", "compute-pool", "account"]


def _scope_callback(scope: Tuple[str, str]):
    if scope[1] is not None and not is_valid_identifier(scope[1]):
        raise ClickException("scope name must be a valid identifier")
    if scope[0] is not None and scope[0].lower() not in VALID_SCOPES:
        raise ClickException(
            f'scope must be one of the following {", ".join(VALID_SCOPES)}'
        )
    return (scope[0].replace("-", " "), scope[1])


ScopeOption = typer.Option(
    (None, None),
    "--in",
    callback=_scope_callback,
    help="Specifies the scope of this command using '--in <scope> <name>' (e.g. list tables --in database my_db). Some object types have specialized scopes (e.g. list service --in compute-pool my_pool)",
)

SUPPORTED_TYPES_MSG = "\n\nSupported types: " + ", ".join(SUPPORTED_OBJECTS)


@app.command(
    "list",
    help=f"Lists all available Snowflake objects of given type.{SUPPORTED_TYPES_MSG}",
)
@with_output
@global_options_with_connection
def list_(
    object_type: str = ObjectArgument,
    like: Optional[str] = LikeOption,
    scope: Optional[Tuple[str, str]] = ScopeOption,
    **options,
):
    return QueryResult(
        ObjectManager().show(object_type=object_type, like=like, scope=scope)
    )


@app.command(
    help=f"Drops Snowflake object of given name and type. {SUPPORTED_TYPES_MSG}"
)
@with_output
@global_options_with_connection
def drop(object_type: str = ObjectArgument, object_name: str = NameArgument, **options):
    return QueryResult(ObjectManager().drop(object_type=object_type, name=object_name))


# Image repository is the only supported object that does not have a DESCRIBE command.
DESCRIBE_SUPPORTED_TYPES_MSG = f"\n\nSupported types: {', '.join(obj for obj in SUPPORTED_OBJECTS if obj != 'image-repository')}"


@app.command(
    help=f"Provides description of an object of given type. {DESCRIBE_SUPPORTED_TYPES_MSG}"
)
@with_output
@global_options_with_connection
def describe(
    object_type: str = ObjectArgument, object_name: str = NameArgument, **options
):
    return QueryResult(
        ObjectManager().describe(object_type=object_type, name=object_name)
    )
