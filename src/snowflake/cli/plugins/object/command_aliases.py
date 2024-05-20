from __future__ import annotations

from typing import List, Optional, Tuple

import typer
from click import ClickException
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.plugins.object.commands import (
    ScopeOption,
    describe,
    drop,
    list_,
    scope_option,  # noqa: F401
)


def add_object_command_aliases(
    app: SnowTyper,
    object_type: ObjectType,
    name_argument: typer.Argument,
    like_option: Optional[typer.Option],
    scope_option: Optional[typer.Option],
    ommit_commands: List[str] = [],
):
    if "list" not in ommit_commands:
        if not like_option:
            raise ClickException('[like_option] have to be defined for "list" command')

        if not scope_option:

            @app.command("list", requires_connection=True)
            def list_cmd(like: str = like_option, **options):  # type: ignore
                list_(
                    object_type=object_type.value.cli_name,
                    like=like,
                    scope=ScopeOption.default,
                    **options,
                )

        else:

            @app.command("list", requires_connection=True)
            def list_cmd(
                like: str = like_option,  # type: ignore
                scope: Tuple[str, str] = scope_option,  # type: ignore
                **options,
            ):
                list_(
                    object_type=object_type.value.cli_name,
                    like=like,
                    scope=scope,
                    **options,
                )

        list_cmd.__doc__ = f"Lists all available {object_type.value.sf_plural_name}."

    if "drop" not in ommit_commands:

        @app.command("drop", requires_connection=True)
        def drop_cmd(name: str = name_argument, **options):
            drop(
                object_type=object_type.value.cli_name,
                object_name=name,
                **options,
            )

        drop_cmd.__doc__ = f"Drops {object_type.value.sf_name} with given name."

    if "describe" not in ommit_commands:

        @app.command("describe", requires_connection=True)
        def describe_cmd(name: str = name_argument, **options):
            describe(
                object_type=object_type.value.cli_name,
                object_name=name,
                **options,
            )

        describe_cmd.__doc__ = f"Provides description of {object_type.value.sf_name}."
