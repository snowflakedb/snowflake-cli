from __future__ import annotations

from typing import Tuple

import typer
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.plugins.object.commands import (
    ScopeOption,
    describe,
    drop,
    list_,
)


def add_object_command_aliases(
    app: SnowTyper,
    object_type: ObjectType,
    name_argument: typer.Argument,
    like_option: typer.Option,
):
    @app.command("list", requires_connection=True)
    def list_cmd(
        like: str = like_option,
        scope: Tuple[str, str] = ScopeOption,
        **options,
    ):
        list_(object_type=object_type.value.cli_name, like=like, scope=scope, **options)

    list_cmd.__doc__ = f"Lists all available {object_type.value.sf_plural_name}."

    @app.command("drop", requires_connection=True)
    def drop_cmd(streamlit_name: str = name_argument, **options):
        drop(
            object_type=object_type.value.cli_name,
            object_name=streamlit_name,
            **options,
        )

    drop_cmd.__doc__ = f"Drops {object_type.value.sf_name} with given name."

    @app.command("describe", requires_connection=True)
    def describe_cmd(streamlit_name: str = name_argument, **options):
        describe(
            object_type=object_type.value.cli_name,
            object_name=streamlit_name,
            **options,
        )

    describe_cmd.__doc__ = f"Provides description of {object_type.value.sf_name}."
