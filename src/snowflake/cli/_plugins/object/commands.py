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

from functools import wraps
from typing import Any, Callable, List, Optional, Tuple

import click
import typer
from click import ClickException
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
from snowflake.cli.api.constants import SUPPORTED_OBJECTS, VALID_SCOPES
from snowflake.cli.api.exceptions import IncompatibleParametersError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import MessageResult, QueryResult
from snowflake.cli.api.project.util import is_valid_identifier

Scope = Tuple[Optional[str], Optional[str]]


class _ScopeParserOption(click.parser.Option):
    """Custom parser Option that consumes 1 or 2 arguments for scope."""

    def __init__(self, *args, **kwargs):
        # Store reference to the Click Option for error messages
        self._click_option = kwargs.pop("click_option", None)
        super().__init__(*args, **kwargs)

    def process(self, value: Any, state: Any) -> None:
        """Process the scope value - value is already a tuple from our custom consumption."""
        state.opts[self.dest] = value
        state.order.append(self.obj)


class ScopeOption(click.Option):
    """Custom Click Option that accepts 1 or 2 arguments for scope.

    Supports:
    - --in account (1 arg, account scope)
    - --in database (1 arg, current database)
    - --in database my_db (2 args, specific database)
    - --in schema (1 arg, current schema)
    - --in schema my_schema (2 args, specific schema)
    """

    default = (None, None)

    def __init__(
        self, *args, help_example: str = "", dest_name: str = "scope", **kwargs
    ):
        self.help_example = help_example
        super().__init__(*args, **kwargs)
        # Override the name to use 'scope' instead of 'in'
        self.name = dest_name

    def type_cast_value(self, ctx: click.Context, value: Any) -> Any:
        """Override to preserve tuple values without conversion."""
        if isinstance(value, tuple):
            return value
        return super().type_cast_value(ctx, value)

    def add_to_parser(self, parser: click.parser.OptionParser, ctx: click.Context):
        parser_opt = _ScopeParserOption(
            obj=self,
            opts=self.opts,
            dest=self.name,
            action="store",
            nargs=1,
            click_option=self,
        )

        for opt in self.opts:
            prefix, value = click.parser.split_opt(opt)
            if len(prefix) == 1 and len(value) == 1:
                parser._short_opt[opt] = parser_opt  # noqa: SLF001
            else:
                normalized = click.parser.normalize_opt(opt, ctx)
                parser._long_opt[normalized] = parser_opt  # noqa: SLF001
            parser._opt_prefixes.add(prefix[0])  # noqa: SLF001

        # Monkey-patch the parser's _get_value_from_state to handle our option specially
        original_get_value = parser._get_value_from_state  # type: ignore[attr-defined]  # noqa: SLF001

        def _custom_get_value(
            option_name: str, option: click.parser.Option, state: Any
        ) -> Any:
            if option is parser_opt:
                rargs = state.rargs

                if not rargs:
                    raise click.BadOptionUsage(
                        option_name,
                        "Missing scope type. Use '--in <scope_type>' or '--in <scope_type> <name>'.",
                        ctx=ctx,
                    )

                scope_type = rargs.pop(0)

                # Check if next argument exists and is not another option
                if rargs and not rargs[0].startswith("-"):
                    scope_name = rargs.pop(0)
                    return (scope_type, scope_name)
                else:
                    # No name provided - valid for account/database/schema
                    return (scope_type, None)
            else:
                return original_get_value(option_name, option, state)

        parser._get_value_from_state = _custom_get_value  # type: ignore[attr-defined]  # noqa: SLF001


def _create_scope_command_class(help_example: str):
    """Create a custom TyperCommand class that includes the ScopeOption."""
    from typer.core import TyperCommand

    class ScopeTyperCommand(TyperCommand):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            scope_opt = ScopeOption(
                ["--in"],
                "scope",
                default=(None, None),
                help=f"Specifies the scope of this command. For example, {help_example}.",
                help_example=help_example,
            )
            self.params.append(scope_opt)

    return ScopeTyperCommand


def with_scope(help_example: str):
    """
    Decorator that adds a variadic --in scope option to a command.

    This decorator injects a custom Click option that can accept 1 or 2 arguments:
    - --in account (1 arg)
    - --in database my_db (2 args)

    The scope value is passed to the command via **options as 'scope'.

    Usage:
        @app.command("list", requires_connection=True)
        @with_scope(help_example="`--in account` or `--in database my_db`")
        def list_(object_type: str, **options):
            scope = options.get("scope", (None, None))
            ...
    """

    def decorator(func: Callable) -> Callable:
        # Store the scope option configuration on the function
        # This will be used to create a custom command class
        if not hasattr(func, "_scope_option_config"):
            func._scope_option_config = {}  # type: ignore[attr-defined]  # noqa: SLF001
        func._scope_option_config["help_example"] = help_example  # type: ignore[attr-defined]  # noqa: SLF001
        func._scope_option_config["cls"] = _create_scope_command_class(help_example)  # type: ignore[attr-defined]  # noqa: SLF001

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        # Copy the config to the wrapper
        wrapper._scope_option_config = func._scope_option_config  # type: ignore[attr-defined]  # noqa: SLF001
        return wrapper

    return decorator


def scope_option(help_example: str):
    """
    Create a scope option for use as a function parameter default.

    This returns a typer.Option that serves as a placeholder. The actual
    variadic behavior is handled by the with_scope decorator.

    For backward compatibility with commands that use:
        scope: Tuple[str, str] = scope_option(help_example="...")
    """
    return typer.Option(
        (None, None),
        "--in",
        help=f"Specifies the scope of this command using '--in <scope> <name>', for example {help_example}.",
    )


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
    help_example='`list function --like "my%"` lists all functions that begin with "my"',
)


def _scope_validate(object_type: str, scope: Scope):
    scope_type, scope_name = scope

    if scope_type is None:
        return

    if scope_type.lower() not in VALID_SCOPES:
        raise ClickException(
            f"Scope must be one of the following: {', '.join(VALID_SCOPES)}."
        )

    # Name validation only applies when a name is provided
    if scope_name is not None and not is_valid_identifier(scope_name):
        raise ClickException("Scope name must be a valid identifier.")

    if scope_type == "compute-pool" and object_type != "service":
        raise ClickException(
            "compute-pool scope is only supported for listing service."
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


SUPPORTED_TYPES_MSG = "\n\nSupported types: " + ", ".join(SUPPORTED_OBJECTS)


@app.command(
    "list",
    help=f"Lists all available Snowflake objects of given type.{SUPPORTED_TYPES_MSG}",
    requires_connection=True,
)
@with_scope(
    help_example="`list table --in account` or `list table --in database my_db`"
)
def list_(
    object_type: str = ObjectArgument,
    like: str = LikeOption,
    terse: Optional[bool] = terse_option_(),
    limit: Optional[int] = limit_option_(),
    **options,
):
    scope = options.get("scope", (None, None))
    if scope is None:
        scope = (None, None)
    _scope_validate(object_type, scope)
    return QueryResult(
        ObjectManager().show(
            object_type=object_type,
            like=like,
            scope=scope,
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
        raise ClickException(
            "Provide either list of object attributes, or object definition in JSON format"
        )

    result = ObjectManager().create(
        object_type=object_type,
        object_data=object_data,
        if_not_exists=if_not_exists,
        replace=replace,
    )
    return MessageResult(result)
