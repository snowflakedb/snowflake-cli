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
from click import types
from snowflake.cli._plugins.dbt.constants import (
    DBT_COMMANDS,
    ENV_FILENAME,
    OUTPUT_COLUMN_NAME,
    PROFILES_FILENAME,
    RESULT_COLUMN_NAME,
)
from snowflake.cli._plugins.dbt.manager import (
    DBTDeployAttributes,
    DBTManager,
    _reject_control_chars,
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
DefaultEnvironmentOption = OverrideableOption(
    None,
    "--default-env",
    mutually_exclusive=["unset_default_env"],
)
UnsetDefaultEnvironmentOption = OverrideableOption(
    False,
    "--unset-default-env",
    mutually_exclusive=["default_env"],
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


def _env_callback(value: Optional[str]) -> Optional[str]:
    return _reject_control_chars(value, "--env")


def _default_env_callback(value: Optional[str]) -> Optional[str]:
    return _reject_control_chars(value, "--default-env")


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
    env_file_dir: Optional[str] = typer.Option(
        help=(
            f"Path to directory containing {ENV_FILENAME}. If provided, the file is "
            f"injected into the deployed project root, overwriting any {ENV_FILENAME} "
            f"present in --source."
        ),
        show_default=False,
        default=None,
        hidden=not FeatureFlag.ENABLE_DBT_PROJECT_ENV_VARS.is_enabled(),
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
    default_env: Optional[str] = DefaultEnvironmentOption(
        help=(
            f"Default environment for the dbt project. "
            f"Selects the environment block from {ENV_FILENAME} that the project "
            f"compiles and executes with by default. "
            f"Mutually exclusive with --unset-default-env."
        ),
        callback=_default_env_callback,
        hidden=not FeatureFlag.ENABLE_DBT_PROJECT_ENV_VARS.is_enabled(),
    ),
    unset_default_env: Optional[bool] = UnsetDefaultEnvironmentOption(
        help="Unset the default environment for the dbt project. Mutually exclusive with --default-env.",
        hidden=not FeatureFlag.ENABLE_DBT_PROJECT_ENV_VARS.is_enabled(),
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
        show_default=False,
        help="dbt Core version to use for the project, for example '1.10.15'. Full list of supported versions can be found at https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-dbt-core-versions",
    ),
    **options,
) -> CommandResult:
    """
    Upload local dbt project files and create or update a DBT project object on Snowflake.

    Examples:
        snow dbt deploy PROJECT
        snow dbt deploy PROJECT --source=/Users/jdoe/project --force
    """
    project_path = SecurePath(source) if source is not None else SecurePath.cwd()
    profiles_dir_path = SecurePath(profiles_dir) if profiles_dir else project_path
    env_file_path = SecurePath(env_file_dir) if env_file_dir else None
    attrs = DBTDeployAttributes(
        default_target=default_target,
        unset_default_target=unset_default_target,
        default_env=default_env,
        unset_default_env=unset_default_env,
        external_access_integrations=external_access_integrations,
        install_local_deps=install_local_deps,
        dbt_version=dbt_version,
    )
    return QueryResult(
        DBTManager().deploy(
            name,
            path=project_path.resolve(),
            profiles_path=profiles_dir_path.resolve(),
            env_file_path=env_file_path.resolve() if env_file_path else None,
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
        show_default=False,
        help="dbt Core version to use for execution (ephemeral, does not change project configuration). Full list of supported versions can be found at https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-dbt-core-versions",
    ),
    environment: Optional[str] = typer.Option(
        None,
        "--env",
        show_default=False,
        callback=_env_callback,
        hidden=not FeatureFlag.ENABLE_DBT_PROJECT_ENV_VARS.is_enabled(),
        help="Selects the target environment from env.yml at execution time. "
        "Use 'NO_ENV' to skip env.yml entirely.",
    ),
    env_vars: Optional[str] = typer.Option(
        None,
        "--env-vars",
        show_default=False,
        hidden=not FeatureFlag.ENABLE_DBT_PROJECT_ENV_VARS.is_enabled(),
        help="Environment variable overrides as a YAML/JSON object, e.g. "
        '\'{"DBT_FOO": "1", "DBT_BAR": "2"}\'. '
        "Values must be strings; numbers, booleans, null, nested objects, "
        "and arrays are rejected (quote scalars, e.g. 'DBT_FOO: \"1\"'). "
        "Keys must start with 'DBT_' and contain only letters, digits, and "
        "underscores. Variables with the DBT_ENV_SECRET_ prefix are accepted "
        "but appear in the SQL text and query history; to avoid that, use "
        "the secrets: block in env.yml.",
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
        environment = ctx.parent.params.get("environment")
        env_vars = ctx.parent.params.get("env_vars")
        execute_args = (
            dbt_command,
            name,
            run_async,
            dbt_version,
            environment,
            env_vars,
            *dbt_cli_args,
        )
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
