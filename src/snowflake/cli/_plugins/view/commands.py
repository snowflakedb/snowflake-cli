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

import typer
from snowflake.cli._plugins.view.manager import ViewManager
from snowflake.cli.api.commands.flags import (
    ReplaceOption,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import QueryResult

app = SnowTyperFactory(
    name="view",
    help="Manages views in Snowflake.",
)

VIEW_IDENTIFIER = identifier_argument(sf_object="view", example="MY_VIEW")
LikeOption = like_option(
    help_example='`list --like "my%"` lists all views that begin with "my"',
)


@app.command("list", requires_connection=True)
def list_(
    like: str = LikeOption,
    **options,
):
    """Lists all views in the current or specified schema."""
    return QueryResult(ViewManager().show(like=like))


@app.command(requires_connection=True)
def create(
    name: FQN = VIEW_IDENTIFIER,
    query: str = typer.Option(
        ...,
        "--query",
        "-q",
        help="SQL query that defines the view.",
        show_default=False,
    ),
    replace: bool = ReplaceOption(),
    **options,
):
    """Creates a view with the given name and defining query."""
    return QueryResult(ViewManager().create(name=name, query=query, replace=replace))
