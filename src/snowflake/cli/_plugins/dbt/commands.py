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
from typing import Optional

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn
from snowflake.cli._plugins.dbt.constants import DBT_COMMANDS
from snowflake.cli._plugins.dbt.manager import DBTManager
from snowflake.cli._plugins.object.command_aliases import add_object_command_aliases
from snowflake.cli._plugins.object.commands import scope_option
from snowflake.cli.api.commands.decorators import global_options_with_connection
from snowflake.cli.api.commands.flags import identifier_argument, like_option
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    QueryResult,
    SingleQueryResult,
)
from snowflake.cli.api.secure_path import SecurePath

app = SnowTyperFactory(
    name="dbt",
    help="Manages dbt on Snowflake projects",
    is_hidden=FeatureFlag.ENABLE_DBT.is_disabled,
)
log = logging.getLogger(__name__)


DBTNameArgument = identifier_argument(sf_object="DBT Project", example="my_pipeline")


add_object_command_aliases(
    app=app,
    object_type=ObjectType.DBT_PROJECT,
    name_argument=DBTNameArgument,
    like_option=like_option(
        help_example='`list --like "my%"` lists all dbt projects that begin with “my”'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
    ommit_commands=["drop", "create", "describe"],
)


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
    **options,
) -> CommandResult:
    """
    Copy dbt files and create or update dbt on Snowflake project.
    """
    if source is None:
        path = SecurePath.cwd()
    else:
        path = SecurePath(source)
    return QueryResult(
        DBTManager().deploy(
            path.resolve(),
            name,
            force=force,
        )
    )


# `execute` is a pass through command group, meaning that all params after command should be passed over as they are,
# suppressing usual CLI behaviour for displaying help or formatting options.
dbt_execute_app = SnowTyperFactory(
    name="execute",
    help="Execute a dbt command on Snowflake",
    subcommand_metavar="DBT_COMMAND",
)
app.add_typer(dbt_execute_app)


@dbt_execute_app.callback()
@global_options_with_connection
def before_callback(
    name: FQN = DBTNameArgument,
    run_async: Optional[bool] = typer.Option(
        False, help="Run dbt command asynchronously and check it's result later."
    ),
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
        run_async = ctx.parent.params["run_async"]
        execute_args = (dbt_command, name, run_async, *dbt_cli_args)
        dbt_manager = DBTManager()

        if run_async is True:
            result = dbt_manager.execute(*execute_args)
            return MessageResult(
                f"Command submitted. You can check the result with `snow sql -q \"select execution_status from table(information_schema.query_history_by_user()) where query_id in ('{result.sfqid}');\"`"
            )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            progress.add_task(description=f"Executing 'dbt {dbt_command}'", total=None)
            result = dbt_manager.execute(*execute_args)
            return SingleQueryResult(result)
