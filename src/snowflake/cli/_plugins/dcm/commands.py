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
from typing import List, Optional

import typer
from snowflake.cli._plugins.dcm.exceptions import (
    InvalidManifestError,
    ManifestConfigurationError,
    ManifestNotFoundError,
)
from snowflake.cli._plugins.dcm.manager import DCMProjectManager
from snowflake.cli._plugins.dcm.models import DCMManifest, TargetContext
from snowflake.cli._plugins.dcm.reporters import (
    AnalyzeReporter,
    PlanReporter,
    RefreshReporter,
    TestReporter,
)
from snowflake.cli._plugins.dcm.utils import mock_dcm_response
from snowflake.cli._plugins.object.command_aliases import add_object_command_aliases
from snowflake.cli._plugins.object.commands import scope_option
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.commands.flags import (
    IdentifierType,
    IfExistsOption,
    IfNotExistsOption,
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
from snowflake.connector.cursor import SnowflakeCursor

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
    help="Target profile from manifest.yml to use. Uses default_target if not specified.",
    show_default=False,
)

save_output_option = typer.Option(
    False,
    "--save-output",
    help="Download plan output files to local 'out/' directory.",
)

optional_dcm_identifier = typer.Argument(
    None,
    help="Identifier of DCM Project. Example: MY_PROJECT. Can be omitted if --target is specified or default_target is defined in manifest.",
    show_default=False,
    click_type=IdentifierType(),
)


def _resolve_target_context(
    identifier: Optional[FQN],
    target: Optional[str],
    source_path: SecurePath,
) -> TargetContext:
    """
    Resolve project identifier and configuration from manifest target.

    - If identifier is provided, it takes precedence over target's project_name
    - Configuration is always resolved from target

    Raises:
        CliError: When manifest is invalid or misconfigured
    """
    try:
        manifest = DCMManifest.load(source_path)
        effective_target = manifest.get_effective_target(target)
    except (InvalidManifestError, ManifestConfigurationError) as e:
        raise CliError(str(e))

    project_id = (
        identifier if identifier else FQN.from_string(effective_target.project_name)
    )
    return TargetContext(
        project_identifier=project_id, configuration=effective_target.templating_config
    )


def _resolve_context_with_required_manifest(
    from_location: SecurePath, identifier: FQN | None, target: str | None
) -> TargetContext:
    try:
        context = _resolve_target_context(identifier, target, from_location)
    except ManifestNotFoundError as e:
        raise CliError(str(e))
    return context


def _resolve_context_with_optional_manifest(
    from_location: SecurePath, identifier: FQN | None, target: str | None
) -> TargetContext:
    try:
        context = _resolve_target_context(identifier, target, from_location)
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
) -> CollectionResult | EmptyResult:
    """
    Process plan result, detecting format and returning appropriate result type.

    For new format (version 2), uses the appropriate plan reporter.
    For old format, returns raw data as CollectionResult.
    """
    rows = list(cursor)
    if not rows:
        return CollectionResult([])

    first_row = rows[0]
    first_value = list(first_row)[0] if first_row else None
    if not first_value:
        return CollectionResult([])

    data = json.loads(first_value)

    # Handle new format with reporter.
    # Uses process_payload (not process) because we need to branch on
    # old vs. new format and return CollectionResult for old format.
    if isinstance(data, dict) and data.get("version", 0) == 2:
        reporter = PlanReporter(command_name=command_name)
        reporter.process_payload(data)
        return EmptyResult()

    # Old format
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
    skip_plan: bool = typer.Option(
        False,
        "--skip-plan",
        help="Skips planning step.",
        hidden=True,
    ),
    **options,
):
    """
    Applies changes defined in DCM Project to Snowflake.
    """
    context = _resolve_context_with_required_manifest(from_location, identifier, target)
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

    return _process_plan_result(result, command_name="deploy")


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
    Plans a DCM Project deployment (validates without executing).
    """
    context = _resolve_context_with_required_manifest(from_location, identifier, target)
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

    return _process_plan_result(result, command_name="plan")


@app.command(requires_connection=True, hidden=True)
def raw_analyze(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    variables: Optional[List[str]] = variables_flag,
    target: Optional[str] = target_option,
    **options,
):
    """Analyzes a DCM Project."""
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
        )

    reporter = AnalyzeReporter()
    reporter.process(result)
    return EmptyResult()


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
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    om = ObjectManager()
    if om.object_exists(object_type="dcm", fqn=project_id):
        message = f"DCM Project '{project_id}' already exists."
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
    Drops a DCM Project with the given name.
    """
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    return QueryResult(
        ObjectManager().drop(object_type="dcm", fqn=project_id, if_exists=if_exists)
    )


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
        help="Name or alias of the deployment to drop. For names containing '$', use single quotes to prevent shell expansion (e.g., 'DEPLOYMENT$1').",
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
    **options,
):
    """
    Refreshes dynamic tables defined in DCM project.
    """
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Refreshing dcm project {project_id}", total=None)
        result = DCMProjectManager().refresh(project_identifier=project_id)

    RefreshReporter().process(result)
    return EmptyResult()


@app.command(requires_connection=True)
@mock_dcm_response("test")
def test(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
):
    """
    Tests all expectations defined in DCM project.
    """
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Testing dcm project {project_id}", total=None)
        result = DCMProjectManager().test(project_identifier=project_id)

    reporter = TestReporter()
    reporter.process(result)
    return EmptyResult()
