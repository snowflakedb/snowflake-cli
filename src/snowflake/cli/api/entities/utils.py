import os
from pathlib import Path
from textwrap import dedent
from typing import Any, List, NoReturn, Optional

import jinja2
from click import ClickException
from snowflake.cli._plugins.nativeapp.artifacts import (
    BundleMap,
    resolve_without_follow,
)
from snowflake.cli._plugins.nativeapp.constants import OWNER_COL
from snowflake.cli._plugins.nativeapp.exceptions import (
    InvalidTemplateInFileError,
    MissingScriptError,
    UnexpectedOwnerError,
)
from snowflake.cli._plugins.nativeapp.utils import verify_exists, verify_no_directories
from snowflake.cli._plugins.stage.diff import (
    DiffResult,
    StagePath,
    compute_stage_diff,
    preserve_from_diff,
    sync_local_diff_with_stage,
    to_stage_path,
)
from snowflake.cli._plugins.stage.utils import print_diff_to_console
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.entities.common import get_sql_executor
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
)
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook
from snowflake.cli.api.project.util import unquote_identifier
from snowflake.cli.api.rendering.sql_templates import (
    choose_sql_jinja_env_based_on_template_syntax,
)
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.connector import ProgrammingError


def generic_sql_error_handler(
    err: ProgrammingError, role: Optional[str] = None, warehouse: Optional[str] = None
) -> NoReturn:
    # Potential refactor: If moving away from Python 3.8 and 3.9 to >= 3.10, use match ... case
    if err.errno == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED:
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                '{role}' may not have access to warehouse '{warehouse}'.
                Please grant usage privilege on warehouse to this role.
                """
            ),
            errno=err.errno,
        )
    elif err.errno == NO_WAREHOUSE_SELECTED_IN_SESSION:
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                Please provide a warehouse for the active session role in your project definition file, config.toml file, or via command line.
                """
            ),
            errno=err.errno,
        )
    elif "does not exist or not authorized" in err.msg:
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                Please check the name of the resource you are trying to query or the permissions of the role you are using to run the query.
                """
            )
        )
    raise err


def ensure_correct_owner(row: dict, role: str, obj_name: str) -> None:
    """
    Check if an object has the right owner role
    """
    actual_owner = row[
        OWNER_COL
    ].upper()  # Because unquote_identifier() always returns uppercase str
    if actual_owner != unquote_identifier(role):
        raise UnexpectedOwnerError(obj_name, role, actual_owner)


def _get_stage_paths_to_sync(
    local_paths_to_sync: List[Path], deploy_root: Path
) -> List[StagePath]:
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


def sync_deploy_root_with_stage(
    console: AbstractConsole,
    deploy_root: Path,
    package_name: str,
    stage_schema: str,
    bundle_map: BundleMap,
    role: str,
    prune: bool,
    recursive: bool,
    stage_fqn: str,
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
        stage_fqn (str): The name of the stage to diff against and upload to.
        local_paths_to_sync (List[Path], optional): List of local paths to sync. Defaults to None to sync all
         local paths. Note that providing an empty list here is equivalent to None.
        print_diff (bool): Whether to print the diff between the local files and the remote stage. Defaults to True

    Returns:
        A `DiffResult` instance describing the changes that were performed.
    """

    sql_executor = get_sql_executor()
    # Does a stage already exist within the application package, or we need to create one?
    # Using "if not exists" should take care of either case.
    console.step(
        f"Checking if stage {stage_fqn} exists, or creating a new one if none exists."
    )
    with sql_executor.use_role(role):
        sql_executor.execute_query(
            f"create schema if not exists {package_name}.{stage_schema}"
        )
        sql_executor.execute_query(
            f"""
                    create stage if not exists {stage_fqn}
                    encryption = (TYPE = 'SNOWFLAKE_SSE')
                    DIRECTORY = (ENABLE = TRUE)"""
        )

    # Perform a diff operation and display results to the user for informational purposes
    if print_diff:
        console.step(
            "Performing a diff between the Snowflake stage and your local deploy_root ('%s') directory."
            % deploy_root.resolve()
        )
    diff: DiffResult = compute_stage_diff(deploy_root, stage_fqn)

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

    # Upload diff-ed files to application package stage
    if diff.has_changes():
        console.step(
            "Updating the Snowflake stage from your local %s directory."
            % deploy_root.resolve(),
        )
        sync_local_diff_with_stage(
            role=role,
            deploy_root_path=deploy_root,
            diff_result=diff,
            stage_fqn=stage_fqn,
        )
    return diff


def _execute_sql_script(
    script_content: str,
    database_name: Optional[str] = None,
) -> None:
    """
    Executing the provided SQL script content.
    This assumes that a relevant warehouse is already active.
    If database_name is passed in, it will be used first.
    """
    try:
        sql_executor = get_sql_executor()
        if database_name is not None:
            sql_executor.execute_query(f"use database {database_name}")
        sql_executor.execute_queries(script_content)
    except ProgrammingError as err:
        generic_sql_error_handler(err)


def execute_post_deploy_hooks(
    console: AbstractConsole,
    project_root: Path,
    post_deploy_hooks: Optional[List[PostDeployHook]],
    deployed_object_type: str,
    database_name: str,
) -> None:
    """
    Executes post-deploy hooks for the given object type.
    While executing SQL post deploy hooks, it first switches to the database provided in the input.
    All post deploy scripts templates will first be expanded using the global template context.
    """
    if not post_deploy_hooks:
        return

    with console.phase(f"Executing {deployed_object_type} post-deploy actions"):
        sql_scripts_paths = []
        for hook in post_deploy_hooks:
            if hook.sql_script:
                sql_scripts_paths.append(hook.sql_script)
            else:
                raise ValueError(
                    f"Unsupported {deployed_object_type} post-deploy hook type: {hook}"
                )

        scripts_content_list = render_script_templates(
            project_root,
            get_cli_context().template_context,
            sql_scripts_paths,
        )

        for index, sql_script_path in enumerate(sql_scripts_paths):
            console.step(f"Executing SQL script: {sql_script_path}")
            _execute_sql_script(
                script_content=scripts_content_list[index],
                database_name=database_name,
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
    scripts_contents = []
    for relpath in scripts:
        script_full_path = SecurePath(project_root) / relpath
        try:
            template_content = script_full_path.read_text(file_size_limit_mb=UNLIMITED)
            env = override_env or choose_sql_jinja_env_based_on_template_syntax(
                template_content, reference_name=relpath
            )
            result = env.from_string(template_content).render(jinja_context)
            scripts_contents.append(result)

        except FileNotFoundError as e:
            raise MissingScriptError(relpath) from e

        except jinja2.TemplateSyntaxError as e:
            raise InvalidTemplateInFileError(relpath, e, e.lineno) from e

        except jinja2.UndefinedError as e:
            raise InvalidTemplateInFileError(relpath, e) from e

    return scripts_contents


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
