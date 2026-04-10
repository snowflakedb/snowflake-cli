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
import json
import logging
from typing import List, Optional

import typer
from snowflake.cli._plugins.connection.util import get_account_identifier
from snowflake.cli._plugins.dcm.exceptions import (
    InvalidManifestError,
    ManifestConfigurationError,
    ManifestNotFoundError,
)
from snowflake.cli._plugins.dcm.manager import DCMProjectManager
from snowflake.cli._plugins.dcm.models import DCMManifest, DCMTarget, TargetContext
from snowflake.cli._plugins.dcm.reporters import (
    AnalyzeReporter,
    PlanReporter,
    RefreshReporter,
    Reporter,
    TestReporter,
)
from snowflake.cli._plugins.dcm.utils import (
    clear_command_artifacts,
    mock_dcm_response,
    save_command_response,
)
from snowflake.cli._plugins.object.command_aliases import add_object_command_aliases
from snowflake.cli._plugins.object.commands import scope_option
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.flags import (
    ForceOption,
    IdentifierType,
    IfExistsOption,
    IfNotExistsOption,
    InteractiveOption,
    LocalDirectoryType,
    identifier_argument,
    like_option,
    variables_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import (
    ObjectType,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CollectionResult,
    EmptyResult,
    MessageResult,
    QueryResult,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutor
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)

app = SnowTyperFactory(
    name="dcm",
    help="Manages DCM Projects in Snowflake.",
    is_hidden=FeatureFlag.ENABLE_SNOWFLAKE_PROJECTS.is_disabled,
)

dcm_identifier = identifier_argument(sf_object="DCM Project", example="MY_PROJECT")
variables_flag = variables_option(
    'Variables for the execution context; for example: `-D "<key>=<value>"`.'
)


def _from_option_callback(value: Optional[SecurePath]) -> SecurePath:
    """Handles None default by returning cwd."""
    return value if value is not None else SecurePath.cwd()


from_option = typer.Option(
    None,
    "--from",
    help="Local directory path containing DCM project files. Omit to use current directory.",
    show_default=False,
    click_type=LocalDirectoryType(),
    callback=_from_option_callback,
)

alias_option = typer.Option(
    None,
    "--alias",
    help="Alias for the deployment.",
    show_default=False,
)

terse_option = typer.Option(
    False,
    "--terse",
    help="Returns only a subset of output columns.",
    show_default=False,
)

limit_option = typer.Option(
    None,
    "--limit",
    help="Limits the maximum number of rows returned.",
    show_default=False,
)

target_option = typer.Option(
    None,
    "--target",
    help="Target profile from `manifest.yml` to use. Uses `default_target` if not specified.",
    show_default=False,
)

save_output_option = typer.Option(
    False,
    "--save-output",
    help="Save command response and artifacts to local 'out/' directory.",
)


optional_dcm_identifier = typer.Argument(
    None,
    help="""
        Identifier of DCM Project. Example: MY_DB.MY_SCHEMA.MY_PROJECT.
        Supports fully qualified (recommended) or simple names.
        If unqualified, it defaults to the connection's database and schema.
        Optional if `--target` or `default_target` is defined in the manifest.
    """,
    show_default=False,
    click_type=IdentifierType(),
)


def _validate_account_identifier(target: DCMTarget) -> None:
    current_account = get_account_identifier(get_cli_context().connection)
    if current_account != target.account_identifier:
        raise CliError(
            f"Account mismatch: manifest target specifies account_identifier '{target.account_identifier}', "
            f"but the current session account is '{current_account}'."
        )


def _validate_project_owner(target: DCMTarget) -> None:
    current_role = SqlExecutor().current_role()
    if current_role and current_role.upper() != target.project_owner:
        raise CliError(
            f"Role mismatch: manifest target specifies project_owner '{target.project_owner}', "
            f"but the current session role is '{current_role}'."
        )


def _resolve_target_context(
    identifier: Optional[FQN],
    target: Optional[str],
    source_path: SecurePath,
    validate_owner: bool = False,
) -> TargetContext:
    """
    Resolve project identifier and configuration from manifest target.

    - If identifier is provided, it takes precedence over target's project_name
    - Configuration is always resolved from target

    Raises:
        CliError: When manifest is invalid or misconfigured
    """
    log.info(
        "Resolving DCM target context (has_identifier=%s, target=%s, source_path=%s).",
        bool(identifier),
        target,
        source_path,
    )
    try:
        manifest = DCMManifest.load(source_path)
        effective_target = manifest.get_effective_target(target)
    except (InvalidManifestError, ManifestConfigurationError) as e:
        log.info("Failed to resolve DCM manifest context: %s.", e)
        raise CliError(str(e))

    project_id = (
        identifier if identifier else FQN.from_string(effective_target.project_name)
    )
    log.info(
        "Resolved DCM target context (project_identifier=%s, has_configuration=%s).",
        project_id,
        bool(effective_target.templating_config),
    )
    _validate_account_identifier(effective_target)
    if validate_owner:
        _validate_project_owner(effective_target)
    return TargetContext(
        project_identifier=project_id,
        configuration=effective_target.templating_config,
    )


def _resolve_context_with_required_manifest(
    from_location: SecurePath,
    identifier: FQN | None,
    target: str | None,
    validate_owner: bool = False,
) -> TargetContext:
    try:
        context = _resolve_target_context(
            identifier, target, from_location, validate_owner=validate_owner
        )
    except ManifestNotFoundError as e:
        raise CliError(str(e))
    return context


def _resolve_context_with_optional_manifest(
    from_location: SecurePath,
    identifier: FQN | None,
    target: str | None,
    validate_owner: bool = False,
) -> TargetContext:
    try:
        context = _resolve_target_context(
            identifier, target, from_location, validate_owner=validate_owner
        )
    except ManifestNotFoundError:
        if not identifier:
            raise CliError(
                "No manifest.yml found. Please provide a project identifier or create a manifest.yml file."
            )
        if target:
            raise CliError(
                f"Cannot use --target '{target}' without a valid manifest.yml."
            )
        context = TargetContext(project_identifier=identifier)
    return context


def _process_plan_result(
    cursor: SnowflakeCursor,
    command_name: str = "plan",
    save_output: bool = False,
) -> CollectionResult | EmptyResult:
    """
    Process plan result, detecting format and returning appropriate result type.

    For new format (version 2), uses the appropriate plan reporter.
    For old format, returns raw data as CollectionResult.
    """
    rows = list(cursor)
    if not rows:
        # TODO: when support for old plan api is removed, move this logic into Reporter class completely
        if save_output:
            save_command_response(command_name, {})
        return CollectionResult([])

    first_row = rows[0]
    first_value = list(first_row)[0] if first_row else None
    if not first_value:
        # TODO: when support for old plan api is removed, move this logic into Reporter class completely
        if save_output:
            save_command_response(command_name, {})
        return CollectionResult([])

    data = json.loads(first_value)

    # Handle new format with reporter.
    # Uses process_payload (not process) because we need to branch on
    # old vs. new format and return CollectionResult for old format.
    if isinstance(data, dict) and data.get("version", 0) == 2:
        log.info(
            "Detected DCM plan result version 2 format.",
        )
        reporter = PlanReporter(save_output=save_output, command_name=command_name)
        reporter.process_payload(data)
        return Reporter.format_aware_result(cursor, first_value)

    # Old format
    log.info("Detected legacy DCM plan result format.")
    if save_output:
        save_command_response(command_name, first_value)
    return CollectionResult(data)


add_object_command_aliases(
    app=app,
    object_type=ObjectType.DCM_PROJECT,
    name_argument=dcm_identifier,
    like_option=like_option(
        help_example='`list --like "my%"` lists all DCM Projects that begin with "my"'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
    ommit_commands=["create", "drop", "describe"],
    terse_option=terse_option,
    limit_option=limit_option,
)


@app.command(requires_connection=True)
def deploy(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    variables: Optional[List[str]] = variables_flag,
    alias: Optional[str] = alias_option,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    skip_plan: bool = typer.Option(
        False,
        "--skip-plan",
        help="Skips planning step.",
        hidden=True,
    ),
    **options,
):
    """
    Deploys local project changes to Snowflake by creating, altering, or dropping objects to match your definition files.
    """
    clear_command_artifacts("deploy")

    context = _resolve_context_with_required_manifest(
        from_location, identifier, target, validate_owner=True
    )
    project_id = context.project_identifier

    manager = DCMProjectManager()
    effective_stage = manager.sync_local_files(
        project_identifier=project_id,
        source_directory=str(from_location.path),
    )

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Deploying dcm project {project_id}", total=None)
        if skip_plan:
            cli_console.warning("Skipping planning step")
        result = manager.deploy(
            project_identifier=project_id,
            configuration=context.configuration,
            from_stage=effective_stage,
            variables=variables,
            alias=alias,
            skip_plan=skip_plan,
        )

    return _process_plan_result(result, command_name="deploy", save_output=save_output)


_PURGE_CONFIRM_COMMAND = "PURGE"
_PURGE_CANCEL_COMMAND = "CANCEL"


def _confirm_purge(project_id: FQN) -> None:
    cli_console.warning(
        f"⚠️  DANGER: This operation will DROP ALL objects managed by DCM Project {project_id}  ⚠️"
    )
    expected_identifier = str(project_id)
    while True:
        user_input = typer.prompt(
            f"Type 'purge {expected_identifier}' to confirm or 'cancel' to abort",
            show_default=False,
        )
        parts = user_input.strip().split(maxsplit=1)
        if not parts:
            continue

        command = parts[0].upper()

        if command == _PURGE_CANCEL_COMMAND:
            raise typer.Abort()

        if command == _PURGE_CONFIRM_COMMAND and len(parts) == 2:
            if parts[1].upper() == expected_identifier.upper():
                return
            cli_console.message(
                f"  Project identifier mismatch. Expected: {expected_identifier}, provided: {parts[1]}"
            )


@app.command(requires_connection=True)
def purge(
    identifier: Optional[FQN] = optional_dcm_identifier,
    alias: Optional[str] = alias_option,
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    skip_plan: bool = typer.Option(
        False,
        "--skip-plan",
        help="Skips planning step.",
        hidden=True,
    ),
    **options,
):
    """
    Drops all the objects managed by the DCM Project, but does not drop the project itself.
    """
    clear_command_artifacts("purge")

    context = _resolve_context_with_optional_manifest(
        from_location, identifier, target, validate_owner=True
    )
    project_id = context.project_identifier

    if not force and not interactive:
        raise CliError(
            "Cannot purge the DCM project non-interactively without --force."
        )
    if not force:
        _confirm_purge(project_id)

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Purging dcm project {project_id}", total=None)
        if skip_plan:
            cli_console.warning("Skipping planning step")
        result = DCMProjectManager().purge(
            project_identifier=project_id,
            alias=alias,
            skip_plan=skip_plan,
        )

    return _process_plan_result(result, command_name="purge", save_output=save_output)


@app.command(requires_connection=True)
@mock_dcm_response("plan")
def plan(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    variables: Optional[List[str]] = variables_flag,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    **options,
):
    """
    Shows what objects would be created, altered, or dropped by the `deploy` command, without applying any changes.
    """
    clear_command_artifacts("plan")

    context = _resolve_context_with_required_manifest(
        from_location, identifier, target, validate_owner=True
    )
    project_id = context.project_identifier

    manager = DCMProjectManager()
    effective_stage = manager.sync_local_files(
        project_identifier=project_id,
        source_directory=str(from_location.path),
    )

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Planning dcm project {project_id}", total=None)
        result = manager.plan(
            project_identifier=project_id,
            configuration=context.configuration,
            from_stage=effective_stage,
            variables=variables,
            save_output=save_output,
        )

    return _process_plan_result(result, command_name="plan", save_output=save_output)


@app.command(requires_connection=True, hidden=True)
def raw_analyze(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    variables: Optional[List[str]] = variables_flag,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    **options,
):
    """Analyzes a DCM Project."""
    clear_command_artifacts("raw-analyze")

    context = _resolve_context_with_required_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    manager = DCMProjectManager()
    effective_stage = manager.sync_local_files(
        project_identifier=project_id,
        source_directory=str(from_location.path),
    )

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Analyzing dcm project {project_id}", total=None)
        result = manager.raw_analyze(
            project_identifier=project_id,
            configuration=context.configuration,
            from_stage=effective_stage,
            variables=variables,
            save_output=save_output,
        )

    reporter = AnalyzeReporter(save_output=save_output)
    return reporter.process(result)


@app.command(requires_connection=True)
def create(
    identifier: Optional[FQN] = optional_dcm_identifier,
    if_not_exists: bool = IfNotExistsOption(
        help="Do nothing if the project already exists."
    ),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
):
    """
    Creates a DCM Project in Snowflake.
    """
    context = _resolve_context_with_optional_manifest(
        from_location, identifier, target, validate_owner=True
    )
    project_id = context.project_identifier

    om = ObjectManager()
    if om.object_exists(object_type="dcm", fqn=project_id):
        message = f"DCM Project '{project_id}' already exists."
        log.info(
            "DCM project already exists during create (project_identifier=%s).",
            project_id,
        )
        if if_not_exists:
            return MessageResult(message)
        raise CliError(message)

    dpm = DCMProjectManager()
    with cli_console.phase(f"Creating DCM Project '{project_id}'"):
        dpm.create(project_identifier=project_id)

    return MessageResult(f"DCM Project '{project_id}' successfully created.")


@app.command(requires_connection=True)
def drop(
    identifier: Optional[FQN] = optional_dcm_identifier,
    if_exists: bool = IfExistsOption(help="Do nothing if the project does not exist."),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
):
    """
    Drops a DCM Project. All the objects deployed and managed by this project won't be dropped.
    """
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    result = QueryResult(
        ObjectManager().drop(object_type="dcm", fqn=project_id, if_exists=if_exists)
    )
    log.info(
        "DCM project %s is deleted (if_exists).",
        project_id,
    )
    return result


@app.command(requires_connection=True)
def describe(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
):
    """
    Provides description of a DCM Project.
    """
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    return QueryResult(ObjectManager().describe(object_type="dcm", fqn=project_id))


@app.command(requires_connection=True)
def list_deployments(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
):
    """
    Lists deployments of given DCM Project.
    """
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    pm = DCMProjectManager()
    results = pm.list_deployments(project_identifier=project_id)
    return QueryResult(results)


@app.command(requires_connection=True)
def drop_deployment(
    identifier: Optional[FQN] = optional_dcm_identifier,
    deployment: str = typer.Option(
        ...,
        "--deployment",
        help="Name or alias of the deployment to drop. For names containing '$', use single quotes to prevent shell expansion (e.g., 'DEPLOYMENT$1'). If both the deployment name and the alias match two different deployments, the deployment name match has higher precedence.",
        show_default=False,
    ),
    if_exists: bool = IfExistsOption(
        help="Do nothing if the deployment does not exist."
    ),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
):
    """
    Drops a deployment from the DCM Project.
    """
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    # Detect potential shell expansion issues
    if deployment and deployment.upper() == "DEPLOYMENT":
        cli_console.warning(
            f"Deployment name '{deployment}' might be truncated due to shell expansion. "
            f"If you meant to use a deployment like 'DEPLOYMENT$1', try using single quotes: 'DEPLOYMENT$1'."
        )

    dpm = DCMProjectManager()
    dpm.drop_deployment(
        project_identifier=project_id,
        deployment_name=deployment,
        if_exists=if_exists,
    )
    log.info(
        "Dropped %s deployment from project %s.",
        deployment,
        project_id,
    )
    return MessageResult(
        f"Deployment '{deployment}' dropped from DCM Project '{project_id}'."
    )


@app.command(requires_connection=True)
def preview(
    identifier: Optional[FQN] = optional_dcm_identifier,
    object_identifier: FQN = typer.Option(
        ...,
        "--object",
        help="FQN of table/view/dynamic table to be previewed.",
        show_default=False,
        click_type=IdentifierType(),
    ),
    from_location: SecurePath = from_option,
    variables: Optional[List[str]] = variables_flag,
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        help="The maximum number of rows to be returned.",
        show_default=False,
    ),
    target: Optional[str] = target_option,
    **options,
):
    """
    Returns rows from any table, view, dynamic table.
    """
    context = _resolve_context_with_required_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    manager = DCMProjectManager()
    effective_stage = manager.sync_local_files(
        project_identifier=project_id,
        source_directory=str(from_location.path),
    )

    with cli_console.spinner() as spinner:
        spinner.add_task(
            description=f"Previewing {object_identifier}.",
            total=None,
        )
        result = manager.preview(
            project_identifier=project_id,
            object_identifier=object_identifier,
            configuration=context.configuration,
            from_stage=effective_stage,
            variables=variables,
            limit=limit,
        )

    return QueryResult(result)


@app.command(requires_connection=True)
@mock_dcm_response("refresh")
def refresh(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    **options,
):
    """
    Refreshes dynamic tables defined in DCM project. It applies only to deployed objects.
    """
    clear_command_artifacts("refresh")

    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Refreshing dcm project {project_id}", total=None)
        result = DCMProjectManager().refresh(project_identifier=project_id)

    reporter = RefreshReporter(save_output=save_output)
    return reporter.process(result)


@app.command(requires_connection=True)
@mock_dcm_response("test")
def test(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    **options,
):
    """
    Tests all expectations defined in DCM project. It applies only to deployed objects.
    """
    clear_command_artifacts("test")

    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Testing dcm project {project_id}", total=None)
        result = DCMProjectManager().test(project_identifier=project_id)

    reporter = TestReporter(save_output=save_output)
    return reporter.process(result)
