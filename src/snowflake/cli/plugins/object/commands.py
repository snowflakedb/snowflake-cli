from __future__ import annotations

from typing import Any, Dict, List, Tuple

import typer
from click import ClickException
from snowflake.cli.api.commands.flags import like_option
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.constants import SUPPORTED_OBJECTS, VALID_SCOPES
from snowflake.cli.api.output.types import MessageResult, QueryResult
from snowflake.cli.api.project.util import is_valid_identifier
from snowflake.cli.plugins.object.manager import ObjectManager

app = SnowTyper(
    name="object",
    help="Manages Snowflake objects like warehouses and stages",
)


NameArgument = typer.Argument(help="Name of the object")
ObjectArgument = typer.Argument(
    help="Type of object. For example table, procedure, streamlit.",
    case_sensitive=False,
    show_default=False,
)
ObjectDefinitionArgument = typer.Argument(
    help="""Object definition in JSON format, for example \'{"name": "my_database", "owner": "owner_role"}\',
or provided as a list of key=value pairs, for example: name=my_database owner=owner_role.
Check https://docs.snowflake.com/LIMITEDACCESS/rest-api/reference/ for the full list of available parameters
for every object.
""",
    show_default=False,
)
LikeOption = like_option(
    help_example='`list function --like "my%"` lists all functions that begin with “my”',
)


def _scope_validate(object_type: str, scope: Tuple[str, str]):
    if scope[1] is not None and not is_valid_identifier(scope[1]):
        raise ClickException("scope name must be a valid identifier")
    if scope[0] is not None and scope[0].lower() not in VALID_SCOPES:
        raise ClickException(
            f"scope must be one of the following: {', '.join(VALID_SCOPES)}"
        )
    if scope[0] == "compute-pool" and object_type != "service":
        raise ClickException("compute-pool scope is only supported for listing service")


def scope_option(help_example: str):
    return typer.Option(
        (None, None),
        "--in",
        help=f"Specifies the scope of this command using '--in <scope> <name>', for example {help_example}.",
    )


ScopeOption = scope_option(
    help_example="`list table --in database my_db`. Some object types have specialized scopes (e.g. list service --in compute-pool my_pool)"
)

SUPPORTED_TYPES_MSG = "\n\nSupported types: " + ", ".join(SUPPORTED_OBJECTS)


@app.command(
    "list",
    help=f"Lists all available Snowflake objects of given type.{SUPPORTED_TYPES_MSG}",
    requires_connection=True,
)
def list_(
    object_type: str = ObjectArgument,
    like: str = LikeOption,
    scope: Tuple[str, str] = ScopeOption,
    **options,
):
    _scope_validate(object_type, scope)
    return QueryResult(
        ObjectManager().show(object_type=object_type, like=like, scope=scope)
    )


@app.command(
    help=f"Drops Snowflake object of given name and type. {SUPPORTED_TYPES_MSG}",
    requires_connection=True,
)
def drop(object_type: str = ObjectArgument, object_name: str = NameArgument, **options):
    return QueryResult(ObjectManager().drop(object_type=object_type, name=object_name))


# Image repository is the only supported object that does not have a DESCRIBE command.
DESCRIBE_SUPPORTED_TYPES_MSG = f"\n\nSupported types: {', '.join(obj for obj in SUPPORTED_OBJECTS if obj != 'image-repository')}"


@app.command(
    help=f"Provides description of an object of given type. {DESCRIBE_SUPPORTED_TYPES_MSG}",
    requires_connection=True,
)
def describe(
    object_type: str = ObjectArgument, object_name: str = NameArgument, **options
):
    return QueryResult(
        ObjectManager().describe(object_type=object_type, name=object_name)
    )


def _parse_object_definition(object_definition: List[str]) -> Dict[str, Any]:
    import json

    def _parse_list_to_dict(object_definition: List[str]) -> Dict[str, Any]:
        payload = {}
        for item in object_definition:
            try:
                key, value = item.split("=", 1)
                # try to parse non-string values
                payload[key] = json.loads(value)
            except json.JSONDecodeError:
                payload[key] = value
            except ValueError:
                raise ClickException(f"expected key=value format, got {item}")
        return payload

    if len(object_definition) != 1:
        return _parse_list_to_dict(object_definition)

    # for list of length 1, prefer json error message
    try:
        return json.loads(object_definition[0])
    except json.JSONDecodeError as json_err:
        try:
            return _parse_list_to_dict(object_definition)
        except ClickException:
            raise json_err


@app.command(name="create", requires_connection=True)
def create(
    object_type: str = ObjectArgument,
    object_definition: List[str] = ObjectDefinitionArgument,
    **options,
):
    """Create an object of a given type. List of supported objects
    and parameters: https://docs.snowflake.com/LIMITEDACCESS/rest-api/reference/"""
    object_data = _parse_object_definition(object_definition)
    result = ObjectManager().create(object_type=object_type, object_data=object_data)
    return MessageResult(result)
