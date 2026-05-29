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

import logging

import typer
from snowflake.cli._plugins.view.manager import ViewManager
from snowflake.cli.api.commands.flags import identifier_argument, like_option
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import CommandResult, QueryResult, SingleQueryResult

app = SnowTyperFactory(
    name="view",
    help="Manages views in Snowflake.",
    is_hidden=FeatureFlag.ENABLE_VIEW_COMMANDS.is_disabled,
)
log = logging.getLogger(__name__)

VIEW_IDENTIFIER = identifier_argument(sf_object="view", example="MY_VIEW")


@app.command("list", requires_connection=True)
def list_views(
    like=like_option(
        help_example='`list --like "my%"` lists all views with names beginning with "my"'
    ),
    **options,
) -> CommandResult:
    """Lists views in the current or specified schema."""
    return QueryResult(ViewManager().show(like=like))


@app.command("create", requires_connection=True)
def create(
    name: FQN = VIEW_IDENTIFIER,
    query: str = typer.Argument(
        ...,
        help="SQL query that defines the view.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Creates a view with the specified query."""
    return SingleQueryResult(ViewManager().create(name=name, query=query))
