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

import json
import logging
import os
from typing import Optional

import typer
from click import types
from snowflake.cli._plugins.dbt.constants import (
    DBT_COMMANDS,
    DBT_PROJECTS_PROFILES_FILENAME,
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
from snowflake.cli._plugins.stage.commands import copy as stage_copy
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

# Alias `snow stage copy` as `snow dbt copy` so users working with a dbt project's
# stage don't have to switch command groups. This registers the exact same command
# function under the dbt app (SnowTyperFactory.command returns the function
# unchanged and builds an independent click command per app), so behavior and flags
# are identical to `snow stage copy` with no side effects on it. The `help=` override
# gives `snow dbt copy` its own description without touching `snow stage copy`.
app.command(
    "copy",
    requires_connection=True,
    help=(
        "Copies files between a local directory and a stage, or between stages "
        "(you provide the full @stage/… path). Behaves exactly like "
        "`snow stage copy`; handy when working with a dbt project's files on a stage."
    ),
)(stage_copy)


def _env_callback(value: Optional[str]) -> Optional[str]:
    return _reject_control_chars(value, "--env")


def _import_callback(value: Optional[list[str]]) -> Optional[list[str]]:
    # Validate each --import value at parse time (before the connection is
    # established) so a malformed value fails fast. Runs the same renderer the
    # SQL builder uses (discarding the result) so validation and rendering can't
    # drift.
    for entry in value or []:
        DBTManager._render_import(entry)  # noqa: SLF001
    return value


def _default_env_callback(value: Optional[str]) -> Optional[str]:
    return _reject_control_chars(value, "--default-env")


def _git_commit_callback(value: Optional[str]) -> Optional[str]:
    return _reject_control_chars(value, "--git-commit")


def _git_branch_callback(value: Optional[str]) -> Optional[str]:
    return _reject_control_chars(value, "--git-branch")


def _github_actions_git_metadata() -> tuple[Optional[str], Optional[str]]:
    """Auto-detect ``(git_commit, git_branch)`` from GitHub Actions env vars.

    Best-effort: returns ``(None, None)`` on any failure (or when not running under
    GitHub Actions) so auto-detection can never block a deploy — an explicit
    ``--git-commit``/``--git-branch`` can always supply the values instead.

    On ``push`` events ``GITHUB_SHA`` / ``GITHUB_REF_NAME`` are the branch-tip
    commit and branch name. On ``pull_request`` events ``GITHUB_SHA`` is an
    ephemeral merge commit (PR branch merged into base) that is not the deployed
    source, so it is never recorded; the commit comes only from the real PR head
    SHA in the event payload (``.pull_request.head.sha``), and the branch from
    ``GITHUB_HEAD_REF``. If the payload can't be read, the commit is left unset
    (``None``) so an explicit ``--git-commit`` can supply it. ``pull_request_target``
    is intentionally not special-cased: it runs in the base-branch context, so its
    ``GITHUB_SHA`` (the base commit) already matches what is deployed.
    """
    if os.getenv("GITHUB_ACTIONS") != "true":
        return None, None

    try:
        if os.getenv("GITHUB_EVENT_NAME") == "pull_request":
            # Feature-branch deploy: the branch is GITHUB_HEAD_REF and the commit is
            # the real PR head SHA from the event payload (GITHUB_SHA here is the
            # ephemeral merge commit). If the payload is unavailable, leave the commit
            # unset so an explicit --git-commit can supply it.
            branch = os.getenv("GITHUB_HEAD_REF") or None
            commit = None
            event_path = os.getenv("GITHUB_EVENT_PATH")
            if event_path and os.path.isfile(event_path):
                with open(event_path, encoding="utf-8") as event_file:
                    payload = json.load(event_file)
                commit = (
                    payload.get("pull_request", {}).get("head", {}).get("sha") or None
                )
        else:
            # push — and everything else, including pull_request_target, which runs in
            # the base-branch context: GITHUB_SHA/GITHUB_REF_NAME are the commit and
            # branch that were actually deployed. On a tag push GITHUB_REF_NAME is the
            # tag (not a branch), so leave the branch unset rather than record a tag.
            commit = os.getenv("GITHUB_SHA") or None
            if os.getenv("GITHUB_REF_TYPE") == "tag":
                branch = None
            else:
                branch = os.getenv("GITHUB_REF_NAME") or None

        return commit, branch
    except Exception:
        # Best-effort: never let auto-detection break the deploy.
        cli_console.warning(
            "Could not auto-detect git metadata from the GitHub Actions environment; "
            "last_deployed_from will omit it. Pass --git-commit/--git-branch to set it "
            "explicitly."
        )
        return None, None


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
        help=(
            f"Path to directory containing {PROFILES_FILENAME}"
            + (
                f" (or {DBT_PROJECTS_PROFILES_FILENAME}, which takes precedence over "
                f"{PROFILES_FILENAME} and is staged under its own name)"
                if FeatureFlag.ENABLE_DBT_PROJECT_PROFILES_FILE_PRECEDENCE.is_enabled()
                else ""
            )
            + ". Defaults to directory provided in --source or current working directory"
        ),
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
        help="Recreates the dbt project object with CREATE OR REPLACE DBT PROJECT. This removes all existing versions and run history.",
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
    default_writeback: Optional[bool] = typer.Option(
        None,
        "--default-writeback/--no-default-writeback",
        show_default=False,
        help="Set the writeback default persisted on the dbt project. Omit to leave "
        "the existing setting unchanged.",
        hidden=not FeatureFlag.ENABLE_DBT_PROJECT_WRITEBACK.is_enabled(),
    ),
    auto_compile: Optional[bool] = typer.Option(
        None,
        "--auto-compile/--no-auto-compile",
        show_default=False,
        help="Set whether the dbt project is compiled on deploy; persisted on the "
        "project and applied to subsequent deploys until changed. Omit to leave the "
        "existing setting unchanged.",
        hidden=not FeatureFlag.ENABLE_DBT_PROJECT_AUTO_COMPILE.is_enabled(),
    ),
    git_commit: Optional[str] = typer.Option(
        None,
        "--git-commit",
        show_default=False,
        help="Git commit hash to record in last_deployed_from metadata when deploying from a plain stage (e.g. SnowCLI temp stage). In GitHub Actions it is auto-detected when not provided.",
        hidden=not FeatureFlag.ENABLE_DBT_GIT_METADATA.is_enabled(),
        callback=_git_commit_callback,
    ),
    git_branch: Optional[str] = typer.Option(
        None,
        "--git-branch",
        show_default=False,
        help="Git branch name to record in last_deployed_from metadata when deploying from a plain stage (e.g. SnowCLI temp stage). In GitHub Actions it is auto-detected when not provided.",
        hidden=not FeatureFlag.ENABLE_DBT_GIT_METADATA.is_enabled(),
        callback=_git_branch_callback,
    ),
    **options,
) -> CommandResult:
    """
    Upload local dbt project files and create or update a DBT project object on Snowflake.

    Examples:
        snow dbt deploy PROJECT
        snow dbt deploy PROJECT --source=/Users/jdoe/project
    """
    project_path = SecurePath(source) if source is not None else SecurePath.cwd()
    profiles_dir_path = SecurePath(profiles_dir) if profiles_dir else project_path
    env_file_path = SecurePath(env_file_dir) if env_file_dir else None

    if not FeatureFlag.ENABLE_DBT_GIT_METADATA.is_enabled():
        git_commit = None
        git_branch = None
    elif git_commit is None or git_branch is None:
        # Explicit flags take precedence; only auto-detect when there's a gap to
        # fill, so we never do needless work (or warn about auto-detection) when the
        # caller already passed both values.
        auto_commit, auto_branch = _github_actions_git_metadata()
        detected = []
        if git_commit is None and auto_commit is not None:
            git_commit = auto_commit
            detected.append(f"commit {auto_commit}")
        if git_branch is None and auto_branch is not None:
            git_branch = auto_branch
            detected.append(f"branch {auto_branch}")
        if detected:
            cli_console.message(
                "Auto-detected git metadata from the GitHub Actions environment ("
                + ", ".join(detected)
                + "); pass --git-commit/--git-branch to override."
            )

    attrs = DBTDeployAttributes(
        default_target=default_target,
        unset_default_target=unset_default_target,
        default_env=default_env,
        unset_default_env=unset_default_env,
        external_access_integrations=external_access_integrations,
        install_local_deps=install_local_deps,
        dbt_version=dbt_version,
        default_writeback=default_writeback,
        auto_compile=auto_compile,
        git_commit=git_commit,
        git_branch=git_branch,
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
        "Keys must be uppercase, start with 'DBT_', and contain only "
        "letters, digits, and underscores. Variables with the "
        "DBT_ENV_SECRET_ prefix are accepted but appear in the SQL text "
        "and query history; to avoid that, use the secrets: block in "
        "env.yml.",
    ),
    use_shell_env_vars: bool = typer.Option(
        False,
        "--use-shell-env-vars",
        show_default=False,
        hidden=not FeatureFlag.ENABLE_DBT_PROJECT_ENV_VARS.is_enabled(),
        help="Forward exported shell environment variables with uppercase "
        "names starting with DBT_ (excluding the DBT_ENV_SECRET_ prefix) as "
        "ENV_VARS=(); non-uppercase or otherwise invalid names are skipped. "
        "Overridden by --env-vars on collisions. WARNING: forwarded values "
        "are embedded as literals in the query and appear in Snowflake query "
        "history. Never put credentials, tokens, passwords, or other "
        "confidential data in shell environment variables with the DBT_ "
        "prefix.",
    ),
    writeback: Optional[bool] = typer.Option(
        None,
        "--writeback/--no-writeback",
        show_default=False,
        hidden=not FeatureFlag.ENABLE_DBT_PROJECT_WRITEBACK.is_enabled(),
        help="Whether to write dbt results back for this run. Must be placed before "
        "the dbt command. Omit to use the project's default.",
    ),
    imports: list[str] = typer.Option(
        [],
        "--import",
        show_default=False,
        hidden=not FeatureFlag.ENABLE_DBT_PROJECT_IMPORTS.is_enabled(),
        callback=_import_callback,
        help="Stage contents to import into the run, as an IMPORTS clause. "
        "Repeatable. Each value is a stage path (@stage/s1), a dbt snow URL "
        "(snow://dbt/db.schema.project/versions/live), or a SYSTEM$ function "
        "(e.g. SYSTEM$DBT_GET_LAST_RUN_TARGET('proj')) — optionally with "
        '"as folder" (an ASCII name of letters, digits, underscores, and '
        "hyphens). Single-quote a value that contains spaces, e.g. "
        "'@\"my stage\"/dir'.",
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
        use_shell_env_vars = ctx.parent.params.get("use_shell_env_vars", False)
        writeback = ctx.parent.params.get("writeback")
        imports = ctx.parent.params.get("imports")
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
            result = dbt_manager.execute(
                *execute_args,
                use_shell_env_vars=use_shell_env_vars,
                writeback=writeback,
                imports=imports,
            )
            return MessageResult(
                f"Command submitted. You can check the result with `snow sql -q \"select execution_status from table(information_schema.query_history_by_user()) where query_id in ('{result.sfqid}');\"`"
            )

        with cli_console.spinner() as spinner:
            spinner.add_task(description=f"Executing 'dbt {dbt_command}'", total=None)
            result = dbt_manager.execute(
                *execute_args,
                use_shell_env_vars=use_shell_env_vars,
                writeback=writeback,
                imports=imports,
            )

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
