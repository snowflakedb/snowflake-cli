import os
from enum import Enum
from pathlib import Path
from typing import Any, List, NoReturn, Optional

import jinja2
from click import ClickException
from snowflake.cli._plugins.nativeapp.exceptions import (
    InvalidTemplateInFileError,
    MissingScriptError,
)
from snowflake.cli._plugins.nativeapp.sf_facade import get_snowflake_facade
from snowflake.cli._plugins.nativeapp.utils import verify_exists, verify_no_directories
from snowflake.cli._plugins.stage.diff import (
    DiffResult,
    StagePathType,
    compute_stage_diff,
    preserve_from_diff,
    sync_local_diff_with_stage,
    to_stage_path,
)
from snowflake.cli._plugins.stage.manager import (
    StageManager,
    StagePathParts,
)
from snowflake.cli._plugins.stage.utils import print_diff_to_console
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.cli_global_context import get_cli_context, span
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
)
from snowflake.cli.api.exceptions import (
    DoesNotExistOrUnauthorizedError,
    NoWarehouseSelectedInSessionError,
    SnowflakeSQLExecutionError,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.metrics import CLICounterField
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook
from snowflake.cli.api.rendering.sql_templates import (
    choose_sql_jinja_env_based_on_template_syntax,
)
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.cli.api.sql_execution import SqlExecutor
from snowflake.cli.api.utils.path_utils import resolve_without_follow
from snowflake.connector import ProgrammingError


def generic_sql_error_handler(err: ProgrammingError) -> NoReturn:
    # Potential refactor: If moving away from Python 3.8 and 3.9 to >= 3.10, use match ... case
    if (
        err.errno == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED
        or "does not exist or not authorized" in err.msg
    ):
        raise DoesNotExistOrUnauthorizedError(msg=err.msg) from err
    elif err.errno == NO_WAREHOUSE_SELECTED_IN_SESSION:
        raise NoWarehouseSelectedInSessionError(msg=err.msg) from err
    raise err


def _get_stage_paths_to_sync(
    local_paths_to_sync: List[Path], deploy_root: Path
) -> List[StagePathType]:
    """
    Takes a list of paths (files and directories), returning a list of all files recursively relative to the deploy root.
    """

    stage_paths = []
    for path in local_paths_to_sync:
        if path.is_dir():
            for current_dir, _dirs, files in os.walk(path):
                for file in files:
                    deploy_path = Path(current_dir, file).relative_to(deploy_root)
                    stage_paths.append(to_stage_path(deploy_path))
        else:
            stage_paths.append(to_stage_path(path.relative_to(deploy_root)))
    return stage_paths


@span("sync_deploy_root_with_stage")
def sync_deploy_root_with_stage(
    console: AbstractConsole,
    deploy_root: Path,
    bundle_map: BundleMap,
    prune: bool,
    recursive: bool,
    stage_path: StagePathParts,
    role: str | None = None,
    package_name: str | None = None,
    local_paths_to_sync: List[Path] | None = None,
    print_diff: bool = True,
) -> DiffResult:
    """
    Ensures that the files on our remote stage match the artifacts we have in
    the local filesystem.

    Args:
        bundle_map (BundleMap): The artifact mapping computed by the `build_bundle` function.
        role (str): The name of the role to use for queries and commands.
        prune (bool): Whether to prune artifacts from the stage that don't exist locally.
        recursive (bool): Whether to traverse directories recursively.
        stage_path (DefaultStagePathParts): stage path object.

        package_name (str): supported for Native App compatibility. Should be None out of Native App context.

        local_paths_to_sync (List[Path], optional): List of local paths to sync. Defaults to None to sync all
        local paths. Note that providing an empty list here is equivalent to None.
        print_diff (bool): Whether to print the diff between the local files and the remote stage. Defaults to True

    Returns:
        A `DiffResult` instance describing the changes that were performed.
    """
    if not package_name:
        # ensure stage exists
        stage_fqn = FQN.from_stage(stage_path.stage)
        console.step(f"Creating stage {stage_fqn} if not exists.")
        StageManager().create(fqn=stage_fqn)
    else:
        # ensure stage exists - nativeapp behavior
        sql_facade = get_snowflake_facade()
        schema = stage_path.schema
        stage_fqn = stage_path.stage
        # Does a stage already exist within the application package, or we need to create one?
        # Using "if not exists" should take care of either case.
        console.step(
            f"Checking if stage {stage_fqn} exists, or creating a new one if none exists."
        )
        if not sql_facade.stage_exists(stage_fqn):
            sql_facade.create_schema(schema, database=package_name)
            sql_facade.create_stage(stage_fqn)

    # Perform a diff operation and display results to the user for informational purposes
    if print_diff:
        console.step(
            f"Performing a diff between the Snowflake stage: {stage_path.path} and your local deploy_root: {deploy_root.resolve()}."
        )

    diff: DiffResult = compute_stage_diff(
        local_root=deploy_root,
        stage_path=stage_path,
    )

    if local_paths_to_sync:
        # Deploying specific files/directories
        resolved_paths_to_sync = [
            resolve_without_follow(p) for p in local_paths_to_sync
        ]
        if not recursive:
            verify_no_directories(resolved_paths_to_sync)

        deploy_paths_to_sync = []
        for resolved_path in resolved_paths_to_sync:
            verify_exists(resolved_path)
            deploy_paths = bundle_map.to_deploy_paths(resolved_path)
            if not deploy_paths:
                if resolved_path.is_dir() and recursive:
                    # No direct artifact mapping found for this path. Check to see
                    # if there are subpaths of this directory that are matches. We
                    # loop over sources because it's likely a much smaller list
                    # than the project directory.
                    for src in bundle_map.all_sources(absolute=True):
                        if resolved_path in src.parents:
                            # There is a source that contains this path, get its dest path(s)
                            deploy_paths.extend(bundle_map.to_deploy_paths(src))

            if not deploy_paths:
                raise ClickException(f"No artifact found for {resolved_path}")
            deploy_paths_to_sync.extend(deploy_paths)

        stage_paths_to_sync = _get_stage_paths_to_sync(
            deploy_paths_to_sync, resolve_without_follow(deploy_root)
        )
        diff = preserve_from_diff(diff, stage_paths_to_sync)
    else:
        # Full deploy
        if not recursive:
            verify_no_directories(deploy_root.resolve().iterdir())

    if not prune:
        files_not_removed = [str(path) for path in diff.only_on_stage]
        diff.only_on_stage = []

        if len(files_not_removed) > 0:
            files_not_removed_str = "\n".join(files_not_removed)
            console.warning(
                f"The following files exist only on the stage:\n{files_not_removed_str}\n\nUse the --prune flag to delete them from the stage."
            )

    if print_diff:
        print_diff_to_console(diff, bundle_map)

    # Upload diff-ed files to the stage
    if diff.has_changes():
        console.step(
            "Updating the Snowflake stage from your local %s directory."
            % deploy_root.resolve(),
        )
        sync_local_diff_with_stage(
            role=role,
            deploy_root_path=deploy_root,
            diff_result=diff,
            stage_full_path=stage_path.full_path,
        )
    return diff


def execute_post_deploy_hooks(
    console: AbstractConsole,
    project_root: Path,
    post_deploy_hooks: Optional[List[PostDeployHook]],
    deployed_object_type: str,
    role_name: str,
    database_name: str,
    warehouse_name: str,
) -> None:
    """
    Executes post-deploy hooks for the given object type.
    While executing SQL post deploy hooks, it first switches to the database provided in the input.
    All post deploy scripts templates will first be expanded using the global template context.
    """
    get_cli_context().metrics.set_counter_default(
        CLICounterField.POST_DEPLOY_SCRIPTS, 0
    )

    if not post_deploy_hooks:
        return

    get_cli_context().metrics.set_counter(CLICounterField.POST_DEPLOY_SCRIPTS, 1)

    with (
        console.phase(f"Executing {deployed_object_type} post-deploy actions"),
        get_cli_context().metrics.span("post_deploy_hooks"),
    ):
        sql_scripts_paths = []
        display_paths = []
        for hook in post_deploy_hooks:
            if hook.sql_script:
                sql_scripts_paths.append(hook.sql_script)
                display_paths.append(hook.display_path)
            else:
                raise ValueError(
                    f"Unsupported {deployed_object_type} post-deploy hook type: {hook}"
                )

        scripts_content_list = render_script_templates(
            project_root,
            get_cli_context().template_context,
            sql_scripts_paths,
        )

        sql_facade = get_snowflake_facade()

        for index, sql_script_path in enumerate(display_paths):
            console.step(f"Executing SQL script: {sql_script_path}")
            sql_facade.execute_user_script(
                queries=scripts_content_list[index],
                script_name=sql_script_path,
                role=role_name,
                warehouse=warehouse_name,
                database=database_name,
            )


def render_script_templates(
    project_root: Path,
    jinja_context: dict[str, Any],
    scripts: List[str],
    override_env: Optional[jinja2.Environment] = None,
) -> List[str]:
    """
    Input:
    - project_root: path to project root
    - jinja_context: a dictionary with the jinja context
    - scripts: list of script paths relative to the project root
    - override_env: optional jinja environment to use for rendering,
      if not provided, the environment will be chosen based on the template syntax
    Returns:
    - List of rendered scripts content
    Size of the return list is the same as the size of the input scripts list.
    """
    return [
        render_script_template(project_root, jinja_context, script, override_env)
        for script in scripts
    ]


def render_script_template(
    project_root: Path,
    jinja_context: dict[str, Any],
    script: str,
    override_env: Optional[jinja2.Environment] = None,
) -> str:
    script_full_path = SecurePath(project_root) / script
    try:
        template_content = script_full_path.read_text(file_size_limit_mb=UNLIMITED)
        env = override_env or choose_sql_jinja_env_based_on_template_syntax(
            template_content, reference_name=script
        )
        return env.from_string(template_content).render(jinja_context)

    except FileNotFoundError as e:
        raise MissingScriptError(script) from e

    except jinja2.TemplateSyntaxError as e:
        raise InvalidTemplateInFileError(script, e, e.lineno) from e

    except jinja2.UndefinedError as e:
        raise InvalidTemplateInFileError(script, e) from e


def validation_item_to_str(item: dict[str, str | int]):
    s = item["message"]
    if item["errorCode"]:
        s = f"{s} (error code {item['errorCode']})"
    return s


def drop_generic_object(
    console: AbstractConsole,
    object_type: str,
    object_name: str,
    role: str,
    cascade: bool = False,
):
    """
    Drop object using the given role.
    """
    sql_executor = get_sql_executor()
    with sql_executor.use_role(role):
        console.step(f"Dropping {object_type} {object_name} now.")
        drop_query = f"drop {object_type} {object_name}"
        if cascade:
            drop_query += " cascade"
        try:
            sql_executor.execute_query(drop_query)
        except:
            raise SnowflakeSQLExecutionError(drop_query)

        console.message(f"Dropped {object_type} {object_name} successfully.")


def print_messages(console: AbstractConsole, cursor_results: list[tuple[str]]):
    """
    Shows messages in the console returned by the CREATE or UPGRADE
    APPLICATION command.
    """
    if not cursor_results:
        return

    messages = [row[0] for row in cursor_results]
    for message in messages:
        console.warning(message)
    console.message("")


def get_sql_executor() -> SqlExecutor:
    """Returns an SQL Executor that uses the connection from the current CLI context"""
    return SqlExecutor()


class EntityActions(str, Enum):
    BUNDLE = "action_bundle"
    DEPLOY = "action_deploy"
    DROP = "action_drop"
    VALIDATE = "action_validate"
    EVENTS = "action_events"
    DIFF = "action_diff"
    GET_URL = "action_get_url"

    VERSION_LIST = "action_version_list"
    VERSION_CREATE = "action_version_create"
    VERSION_DROP = "action_version_drop"

    RELEASE_DIRECTIVE_UNSET = "action_release_directive_unset"
    RELEASE_DIRECTIVE_SET = "action_release_directive_set"
    RELEASE_DIRECTIVE_LIST = "action_release_directive_list"
    RELEASE_DIRECTIVE_ADD_ACCOUNTS = "action_release_directive_add_accounts"
    RELEASE_DIRECTIVE_REMOVE_ACCOUNTS = "action_release_directive_remove_accounts"

    RELEASE_CHANNEL_LIST = "action_release_channel_list"
    RELEASE_CHANNEL_ADD_ACCOUNTS = "action_release_channel_add_accounts"
    RELEASE_CHANNEL_REMOVE_ACCOUNTS = "action_release_channel_remove_accounts"
    RELEASE_CHANNEL_ADD_VERSION = "action_release_channel_add_version"
    RELEASE_CHANNEL_REMOVE_VERSION = "action_release_channel_remove_version"
    RELEASE_CHANNEL_SET_ACCOUNTS = "action_release_channel_set_accounts"

    PUBLISH = "action_publish"

    @property
    def get_action_name(self):
        return self.value.replace("action_", "")
