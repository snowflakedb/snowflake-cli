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
from click import ClickException
from snowflake.cli._plugins.object.commands import (
    ScopeOption,
    describe,
    drop,
    list_,
    scope_option,  # noqa: F401
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN


def add_object_command_aliases(
    app: SnowTyperFactory,
    object_type: ObjectType,
    name_argument: typer.Argument,
    like_option: Optional[typer.Option],
    scope_option: Optional[typer.Option],
    ommit_commands: Optional[List[str]] = None,
):
    if ommit_commands is None:
        ommit_commands = list()
    if "list" not in ommit_commands:
        if not like_option:
            raise ClickException('[like_option] have to be defined for "list" command')

        if not scope_option:

            @app.command("list", requires_connection=True)
            def list_cmd(like: str = like_option, **options):  # type: ignore
                return list_(
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
                return list_(
                    object_type=object_type.value.cli_name,
                    like=like,
                    scope=scope,
                    **options,
                )

        list_cmd.__doc__ = f"Lists all available {object_type.value.sf_plural_name}."

    if "drop" not in ommit_commands:

        @app.command("drop", requires_connection=True)
        def drop_cmd(name: FQN = name_argument, **options):
            return drop(
                object_type=object_type.value.cli_name,
                object_name=name,
                **options,
            )

        drop_cmd.__doc__ = f"Drops {object_type.value.sf_name} with given name."

    if "describe" not in ommit_commands:

        @app.command("describe", requires_connection=True)
        def describe_cmd(name: FQN = name_argument, **options):
            return describe(
                object_type=object_type.value.cli_name,
                object_name=name,
                **options,
            )

        describe_cmd.__doc__ = f"Provides description of {object_type.value.sf_name}."
