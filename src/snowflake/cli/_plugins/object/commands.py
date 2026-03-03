# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from typing import List, Optional, Tuple

import typer
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.commands.flags import (
    IdentifierType,
    IfExistsOption,
    IfNotExistsOption,
    ReplaceOption,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.constants import (
    INTEGRATION_OBJECTS,
    SUPPORTED_OBJECTS,
    VALID_SCOPES,
)
from snowflake.cli.api.exceptions import CliError, IncompatibleParametersError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import MessageResult, QueryResult
from snowflake.cli.api.project.util import is_valid_identifier

app = SnowTyperFactory(
    name="object",
    help="Manages Snowflake objects like warehouses and stages",
)


NameArgument = typer.Argument(
    help="Name of the object.", show_default=False, click_type=IdentifierType()
)
ObjectArgument = typer.Argument(
    help="Type of object. For example table, database, compute-pool.",
    case_sensitive=False,
    show_default=False,
)
# TODO: add documentation link
ObjectAttributesArgument = typer.Argument(
    None,
    help="""Object attributes provided as a list of key=value pairs,
for example name=my_db comment='created with Snowflake CLI'.

Check documentation for the full list of available parameters
for every object.
""",
    show_default=False,
)
# TODO: add documentation link
ObjectDefinitionJsonOption = typer.Option(
    None,
    "--json",
    help="""Object definition in JSON format, for example
\'{"name": "my_db", "comment": "created with Snowflake CLI"}\'.

Check documentation for the full list of available parameters
for every object.
""",
    show_default=False,
)
LikeOption = like_option(
    help_example='`list function --like "my%"` lists all functions that begin with “my”',
)


def _get_effective_scope(
    scope: Tuple[str, str], in_account: bool, object_type: str
) -> Tuple[Optional[str], Optional[str]]:
    """Resolve the effective scope from --in and --in-account flags."""
    scope_provided = scope[0] is not None
    if in_account and scope_provided:
        raise IncompatibleParametersError(["--in-account", "--in"])
    if in_account:
        if object_type in INTEGRATION_OBJECTS:
            # SHOW INTEGRATIONS does not support the IN ACCOUNT phrase but these are account level objects anyway
            # so (None, None) is functionally the same
            return (None, None)
        return ("account", None)
    return scope


def _scope_validate(object_type: str, scope: Tuple[Optional[str], Optional[str]]):
    scope_type, scope_name = scope

    if scope_type is None:
        return

    scope_type = scope_type.lower()

    # Account scope is valid but doesn't need name validation
    if scope_type == "account":
        return

    if scope_type not in VALID_SCOPES:
        raise CliError(
            f"Scope type must be one of the following: {', '.join(VALID_SCOPES)}."
        )

    if scope_name is not None and not is_valid_identifier(scope_name):
        raise CliError("Scope name must be a valid identifier.")

    if scope_type == "compute-pool" and object_type != "service":
        raise CliError("compute-pool scope is only supported for listing service.")


def scope_option(help_example: str):
    return typer.Option(
        (None, None),
        "--in",
        help=f"Specifies the scope of this command using '--in <scope> <name>', for example {help_example}.",
    )


def in_account_option_():
    return typer.Option(
        None,
        "--in-account",
        is_flag=True,
        help="Lists objects across the entire account.",
    )


def terse_option_():
    return typer.Option(
        None,
        "--terse",
        help=f"Returns only a subset of available columns.",
        hidden=True,
    )


def limit_option_():
    return typer.Option(
        None,
        "--limit",
        help=f"Limits the maximum number of rows returned.",
        hidden=True,
    )


ScopeOption = scope_option(
    help_example="`list table --in database my_db`. Some object types have specialized scopes (e.g. `list service --in compute-pool my_pool`)"
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
    in_account: bool = in_account_option_(),
    terse: Optional[bool] = terse_option_(),
    limit: Optional[int] = limit_option_(),
    **options,
):
    effective_scope = _get_effective_scope(scope, in_account, object_type)
    _scope_validate(object_type, effective_scope)
    return QueryResult(
        ObjectManager().show(
            object_type=object_type,
            like=like,
            scope=effective_scope,
            terse=terse,
            limit=limit,
        )
    )


@app.command(
    help=f"Drops Snowflake object of given name and type. {SUPPORTED_TYPES_MSG}",
    requires_connection=True,
)
def drop(
    object_type: str = ObjectArgument,
    object_name: FQN = NameArgument,
    if_exists: bool = IfExistsOption(),
    **options,
):
    return QueryResult(
        ObjectManager().drop(
            object_type=object_type, fqn=object_name, if_exists=if_exists
        )
    )


# Image repository is the only supported object that does not have a DESCRIBE command.
DESCRIBE_SUPPORTED_TYPES_MSG = f"\n\nSupported types: {', '.join(obj for obj in SUPPORTED_OBJECTS if obj != 'image-repository')}"


@app.command(
    help=f"Provides description of an object of given type. {DESCRIBE_SUPPORTED_TYPES_MSG}",
    requires_connection=True,
)
def describe(
    object_type: str = ObjectArgument, object_name: FQN = NameArgument, **options
):
    return QueryResult(
        ObjectManager().describe(object_type=object_type, fqn=object_name)
    )


@app.command(name="create", requires_connection=True)
def create(
    object_type: str = ObjectArgument,
    object_attributes: Optional[List[str]] = ObjectAttributesArgument,
    object_json: str = ObjectDefinitionJsonOption,
    if_not_exists: bool = IfNotExistsOption(),
    replace: bool = ReplaceOption(),
    **options,
):
    """
    Create an object of a given type. Check documentation for the list of supported objects
    and parameters.
    """
    import json

    if object_attributes and object_json:
        raise IncompatibleParametersError(["object_attributes", "--json"])

    if object_json:
        object_data = json.loads(object_json)
    elif object_attributes:

        def _parse_if_json(value: str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value

        object_data = {
            v.key: _parse_if_json(v.value)
            for v in parse_key_value_variables(object_attributes)
        }

    else:
        raise CliError(
            "Provide either list of object attributes, or object definition in JSON format"
        )

    result = ObjectManager().create(
        object_type=object_type,
        object_data=object_data,
        if_not_exists=if_not_exists,
        replace=replace,
    )
    return MessageResult(result)
