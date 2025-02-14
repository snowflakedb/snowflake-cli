# Copyright (c) 2025 Snowflake Inc.
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
from snowflake.cli._plugins.dbt.manager import DBTManager
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, QueryResult

app = SnowTyperFactory(
    name="dbt",
    help="Manages dbt on Snowflake projects",
    is_hidden=lambda: True,
)
log = logging.getLogger(__name__)


@app.command(
    "list",
    requires_connection=True,
)
def list_dbts(
    **options,
) -> CommandResult:
    """
    List all dbt projects on Snowflake.
    """
    return QueryResult(DBTManager().list())


@app.command(
    "execute",
    requires_connection=True,
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def execute(
    ctx: typer.Context,
    dbt_command: str = typer.Argument(
        help="dbt command to execute, i. e. run, compile, seed...",
    ),
    name: str = typer.Option(
        default=...,
        help="Name of the dbt object to execute command on.",
    ),
    **options,
) -> CommandResult:
    """
    Execute command on dbt in Snowflake project.
    """
    # ctx.args are parameters that were not captured as known cli params (those are in **options).
    # as a consequence, we don't support passing params known to snowflake cli further to dbt
    dbt_cli_args = ctx.args
    return QueryResult(DBTManager().execute(dbt_command, name, *dbt_cli_args))
