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
from pathlib import Path
from typing import Optional

import typer
from snowflake.cli._plugins.dbt.constants import DBT_COMMANDS
from snowflake.cli._plugins.dbt.manager import DBTManager
from snowflake.cli.api.commands.decorators import global_options_with_connection
from snowflake.cli.api.commands.flags import identifier_argument
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import CommandResult, QueryResult

app = SnowTyperFactory(
    name="dbt",
    help="Manages dbt on Snowflake projects",
    is_hidden=FeatureFlag.ENABLE_DBT_POC.is_disabled,
)
log = logging.getLogger(__name__)


DBTNameArgument = identifier_argument(sf_object="DBT Object", example="my_pipeline")


@app.command(
    "list",
    requires_connection=True,
)
def list_dbts(
    **options,
) -> CommandResult:
    """
    List all dbt on Snowflake projects.
    """
    return QueryResult(DBTManager().list())


@app.command(
    "deploy",
    requires_connection=True,
)
def deploy_dbt(
    name: FQN = DBTNameArgument,
    source: Optional[str] = typer.Option(
        help="Path to directory containing dbt files to deploy. Defaults to current working directory.",
        show_default=False,
        default=None,
    ),
    force: Optional[bool] = typer.Option(
        False,
        help="Overwrites conflicting files in the project, if any.",
    ),
    dbt_version: Optional[str] = typer.Option(
        None,
        help="Version of dbt tool to be used. Taken from dbt_project.yml if not provided.",
    ),
    dbt_adapter_version: Optional[str] = typer.Option(
        None,
        help="dbt-snowflake adapter version to be used",
    ),
    execute_in_warehouse: Optional[str] = typer.Option(
        None, help="Warehouse to use when running `dbt execute` commands"
    ),
    **options,
) -> CommandResult:
    """
    Copy dbt files and create or update dbt on Snowflake project.
    """
    if source is None:
        path = Path.cwd()
    else:
        path = Path(source)
    return QueryResult(
        DBTManager().deploy(
            path.resolve(),
            name,
            dbt_version,
            dbt_adapter_version,
            execute_in_warehouse,
            force=force,
        )
    )


# `execute` is a pass through command group, meaning that all params after command should be passed over as they are,
# suppressing usual CLI behaviour for displaying help or formatting options.
dbt_execute_app = SnowTyperFactory(
    name="execute",
    help="Execute a dbt command on Snowflake",
)
app.add_typer(dbt_execute_app)


@dbt_execute_app.callback()
@global_options_with_connection
def before_callback(
    name: FQN = DBTNameArgument,
    **options,
):
    """Handles global options passed before the command and takes pipeline name to be accessed through child context later"""
    pass


for cmd in DBT_COMMANDS:

    @dbt_execute_app.command(
        name=cmd,
        requires_connection=False,
        requires_global_options=False,
        context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
        help=f"Execute {cmd} command on Snowflake.",
        add_help_option=False,
    )
    def _dbt_execute(
        ctx: typer.Context,
    ) -> CommandResult:
        dbt_cli_args = ctx.args
        dbt_command = ctx.command.name
        name = ctx.parent.params["name"]
        return QueryResult(DBTManager().execute(dbt_command, name, *dbt_cli_args))
