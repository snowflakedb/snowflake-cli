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
import re
from typing import Optional

import click
import typer
from click import types
from snowflake.cli._plugins.dbt.constants import (
    DBT_COMMANDS,
    OUTPUT_COLUMN_NAME,
    PROFILES_FILENAME,
    RESULT_COLUMN_NAME,
)
from snowflake.cli._plugins.dbt.manager import (
    DBTDeployAttributes,
    DBTManager,
)
from snowflake.cli._plugins.object.command_aliases import add_object_command_aliases
from snowflake.cli._plugins.object.commands import scope_option
from snowflake.cli.api.commands.decorators import global_options_with_connection
from snowflake.cli.api.commands.flags import identifier_argument, like_option
from snowflake.cli.api.commands.overrideable_parameter import OverrideableOption
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    QueryResult,
)
from snowflake.cli.api.secure_path import SecurePath

app = SnowTyperFactory(
    name="dbt",
    help="Manages dbt on Snowflake projects.",
)
log = logging.getLogger(__name__)


DBTNameArgument = identifier_argument(sf_object="DBT Project", example="my_pipeline")

# in passthrough commands we need to support that user would either provide the name of dbt object or name of dbt
# command, in which case FQN validation could fail
DBTNameOrCommandArgument = identifier_argument(
    sf_object="DBT Project", example="my_pipeline", click_type=types.StringParamType()
)
DefaultTargetOption = OverrideableOption(
    None,
    "--default-target",
    mutually_exclusive=["unset_default_target"],
)
UnsetDefaultTargetOption = OverrideableOption(
    False,
    "--unset-default-target",
    mutually_exclusive=["default_target"],
)

add_object_command_aliases(
    app=app,
    object_type=ObjectType.DBT_PROJECT,
    name_argument=DBTNameArgument,
    like_option=like_option(
        help_example='`list --like "my%"` lists all dbt projects that begin with "my"'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
    ommit_commands=["create"],
)


SEMANTIC_VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+(-[a-zA-Z0-9]+)?$")


class SemanticVersionType(click.ParamType):
    """Custom Click type that validates semantic version format (major.minor.patch or major.minor.patch-string)."""

    name = "TEXT"

    def convert(self, value, param, ctx):
        if value is None:
            return None
        if not isinstance(value, str):
            self.fail(f"Expected string, got {type(value).__name__}.", param, ctx)
        if not SEMANTIC_VERSION_PATTERN.match(value):
            self.fail(
                f"Invalid version format '{value}'. Expected format: major.minor.patch or major.minor.patch-string (e.g., '1.9.4' or '2.0.0-preview').",
                param,
                ctx,
            )
        return value


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
    profiles_dir: Optional[str] = typer.Option(
        help=f"Path to directory containing {PROFILES_FILENAME}. Defaults to directory provided in --source or current working directory",
        show_default=False,
        default=None,
    ),
    force: Optional[bool] = typer.Option(
        False,
        help="Overwrites conflicting files in the project, if any.",
    ),
    default_target: Optional[str] = DefaultTargetOption(
        help="Default target for the dbt project. Mutually exclusive with --unset-default-target.",
    ),
    unset_default_target: Optional[bool] = UnsetDefaultTargetOption(
        help="Unset the default target for the dbt project. Mutually exclusive with --default-target.",
    ),
    external_access_integrations: Optional[list[str]] = typer.Option(
        None,
        "--external-access-integration",
        show_default=False,
        help="External access integration to be used by the dbt object.",
    ),
    install_local_deps: Optional[bool] = typer.Option(
        False,
        "--install-local-deps",
        show_default=False,
        help="Installs local dependencies from project that don't require external access.",
    ),
    dbt_version: Optional[str] = typer.Option(
        None,
        "--dbt-version",
        click_type=SemanticVersionType(),
        show_default=False,
        hidden=not FeatureFlag.ENABLE_DBT_VERSION.is_enabled(),
        help="dbt version to use for the project, for example '1.9.4'.",
    ),
    **options,
) -> CommandResult:
    """
    Upload local dbt project files and create or update a DBT project object on Snowflake.

    Examples:
        snow dbt deploy PROJECT
        snow dbt deploy PROJECT --source=/Users/jdoe/project --force
    """
    if not FeatureFlag.ENABLE_DBT_VERSION.is_enabled():
        dbt_version = None
    project_path = SecurePath(source) if source is not None else SecurePath.cwd()
    profiles_dir_path = SecurePath(profiles_dir) if profiles_dir else project_path
    attrs = DBTDeployAttributes(
        default_target=default_target,
        unset_default_target=unset_default_target,
        external_access_integrations=external_access_integrations,
        install_local_deps=install_local_deps,
        dbt_version=dbt_version,
    )
    return QueryResult(
        DBTManager().deploy(
            name,
            path=project_path.resolve(),
            profiles_path=profiles_dir_path.resolve(),
            force=force,
            attrs=attrs,
        )
    )


dbt_execute_app = SnowTyperFactory(
    name="execute",
    help="Execute a dbt command on Snowflake. Subcommand name and all "
    "parameters following it will be passed over to dbt.",
    subcommand_metavar="DBT_COMMAND",
)
app.add_typer(dbt_execute_app)


@dbt_execute_app.callback()
@global_options_with_connection
def before_callback(
    name: str = DBTNameOrCommandArgument,
    run_async: Optional[bool] = typer.Option(
        False, help="Run dbt command asynchronously and check it's result later."
    ),
    dbt_version: Optional[str] = typer.Option(
        None,
        "--dbt-version",
        click_type=SemanticVersionType(),
        show_default=False,
        hidden=not FeatureFlag.ENABLE_DBT_VERSION.is_enabled(),
        help="dbt version to use for execution (ephemeral, does not change project configuration).",
    ),
    **options,
):
    """Handles global options passed before the command and takes pipeline name to be accessed through child context later."""
    pass


for cmd in DBT_COMMANDS:

    @dbt_execute_app.command(
        name=cmd,
        requires_connection=False,
        requires_global_options=False,
        context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
        help=f"Execute {cmd} command on Snowflake. Command name and all parameters following it will be passed over to dbt.",
        add_help_option=False,
    )
    def _dbt_execute(
        ctx: typer.Context,
    ) -> CommandResult:
        dbt_cli_args = ctx.args
        dbt_command = ctx.command.name
        name = FQN.from_string(ctx.parent.params["name"])
        run_async = ctx.parent.params["run_async"]
        dbt_version = ctx.parent.params.get("dbt_version")
        if not FeatureFlag.ENABLE_DBT_VERSION.is_enabled():
            dbt_version = None
        execute_args = (dbt_command, name, run_async, dbt_version, *dbt_cli_args)
        dbt_manager = DBTManager()

        if run_async is True:
            result = dbt_manager.execute(*execute_args)
            return MessageResult(
                f"Command submitted. You can check the result with `snow sql -q \"select execution_status from table(information_schema.query_history_by_user()) where query_id in ('{result.sfqid}');\"`"
            )

        with cli_console.spinner() as spinner:
            spinner.add_task(description=f"Executing 'dbt {dbt_command}'", total=None)
            result = dbt_manager.execute(*execute_args)

            try:
                columns = [column.name for column in result.description]
                success_column_index = columns.index(RESULT_COLUMN_NAME)
                stdout_column_index = columns.index(OUTPUT_COLUMN_NAME)
            except ValueError:
                raise CliError("Malformed server response")
            try:
                is_success, output = [
                    (row[success_column_index], row[stdout_column_index])
                    for row in result
                ][-1]
            except IndexError:
                raise CliError("No data returned from server")

            if is_success is True:
                return MessageResult(output)
            else:
                raise CliError(output)
