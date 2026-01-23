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
from dataclasses import dataclass
from typing import List, Optional

import typer
from snowflake.cli._plugins.dcm.manager import DCMProjectManager
from snowflake.cli._plugins.dcm.reporters import RefreshReporter, TestReporter
from snowflake.cli._plugins.dcm.utils import mock_dcm_response
from snowflake.cli._plugins.object.command_aliases import add_object_command_aliases
from snowflake.cli._plugins.object.commands import scope_option
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.commands.flags import (
    IdentifierType,
    IfExistsOption,
    IfNotExistsOption,
    OverrideableOption,
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
    EmptyResult,
    MessageResult,
    QueryJsonValueResult,
    QueryResult,
)
from snowflake.cli.api.utils.path_utils import is_stage_path

app = SnowTyperFactory(
    name="dcm",
    help="Manages DCM Projects in Snowflake.",
    is_hidden=FeatureFlag.ENABLE_SNOWFLAKE_PROJECTS.is_disabled,
)

dcm_identifier = identifier_argument(sf_object="DCM Project", example="MY_PROJECT")
variables_flag = variables_option(
    'Variables for the execution context; for example: `-D "<key>=<value>"`.'
)
configuration_flag = typer.Option(
    None,
    "--configuration",
    help="Configuration of the DCM Project to use. If not specified default configuration is used.",
    show_default=False,
)
from_option = typer.Option(
    None,
    "--from",
    help="Source location: stage path (starting with '@') or local directory path. Omit to use current directory.",
    show_default=False,
)

alias_option = typer.Option(
    None,
    "--alias",
    help="Alias for the deployment.",
    show_default=False,
)
output_path_option = OverrideableOption(
    None,
    "--output-path",
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

optional_dcm_identifier = typer.Argument(
    None,
    help="Identifier of DCM Project. Example: MY_PROJECT. Can be omitted if --target is specified or default_target is defined in manifest.",
    show_default=False,
    click_type=IdentifierType(),
)


@dataclass
class TargetContext:
    """Resolved context from target configuration."""

    project_identifier: FQN
    configuration: Optional[str] = None
    output_path: Optional[str] = None


def _resolve_target_context(
    identifier: Optional[FQN],
    target: Optional[str],
    source_directory: Optional[str] = None,
    use_config: bool = True,
    use_output_path: bool = False,
) -> TargetContext:
    """
    Resolve effective project identifier, configuration, and output_path from target.

    Priority:
    1. Explicit identifier argument overrides target's project_name
    2. Target's templating_config is used if no explicit --configuration is provided
    3. Target's output_path is used if no explicit --output-path is provided
    """
    from snowflake.cli.api.secure_path import SecurePath

    # If identifier is explicitly provided, use it directly
    if identifier:
        return TargetContext(project_identifier=identifier)

    # Try to load manifest and resolve target
    source_path = (
        SecurePath(source_directory).resolve() if source_directory else SecurePath.cwd()
    )

    try:
        manifest = DCMProjectManager.load_manifest(source_path)
    except CliError:
        if target:
            raise CliError(
                f"Cannot use --target '{target}' without a valid manifest.yml in the project directory."
            )
        raise CliError(
            "No project identifier specified and no manifest.yml found to resolve target."
        )

    # Get effective target (explicit or default)
    try:
        effective_target = manifest.get_effective_target(target)
    except CliError:
        if not target and not manifest.default_target:
            raise CliError(
                "No project identifier specified, no --target provided, and no default_target defined in manifest."
            )
        raise

    context = TargetContext(
        project_identifier=FQN.from_string(effective_target.project_name)
    )

    if use_config and effective_target.templating_config:
        context.configuration = effective_target.templating_config

    if use_output_path and effective_target.output_path:
        context.output_path = effective_target.output_path

    return context


add_object_command_aliases(
    app=app,
    object_type=ObjectType.DCM_PROJECT,
    name_argument=dcm_identifier,
    like_option=like_option(
        help_example='`list --like "my%"` lists all DCM Projects that begin with "my"'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
    ommit_commands=["create"],
    terse_option=terse_option,
    limit_option=limit_option,
)


@app.command(requires_connection=True)
def deploy(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: Optional[str] = from_option,
    variables: Optional[List[str]] = variables_flag,
    configuration: Optional[str] = configuration_flag,
    alias: Optional[str] = alias_option,
    target: Optional[str] = target_option,
    skip_plan: bool = typer.Option(
        False,
        "--skip-plan",
        help="Skips planning step",
        hidden=True,
    ),
    **options,
):
    """
    Applies changes defined in DCM Project to Snowflake.
    """
    context = _resolve_target_context(
        identifier, target, from_location, use_config=True, use_output_path=False
    )
    effective_configuration = configuration or context.configuration
    project_id = context.project_identifier

    manager = DCMProjectManager()
    effective_stage = _get_effective_stage(project_id, from_location)

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Deploying dcm project {project_id}", total=None)
        if skip_plan:
            cli_console.warning("Skipping planning step")
        result = manager.deploy(
            project_identifier=project_id,
            configuration=effective_configuration,
            from_stage=effective_stage,
            variables=variables,
            alias=alias,
            skip_plan=skip_plan,
        )
    return QueryJsonValueResult(result)


@app.command(requires_connection=True)
def plan(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: Optional[str] = from_option,
    variables: Optional[List[str]] = variables_flag,
    configuration: Optional[str] = configuration_flag,
    output_path: Optional[str] = output_path_option(
        help="Path where the deployment plan output will be stored. Can be a stage path (starting with '@') or a local directory path."
    ),
    target: Optional[str] = target_option,
    **options,
):
    """
    Plans a DCM Project deployment (validates without executing).
    """
    context = _resolve_target_context(
        identifier, target, from_location, use_config=True, use_output_path=True
    )
    effective_configuration = configuration or context.configuration
    effective_output_path = output_path or context.output_path
    project_id = context.project_identifier

    manager = DCMProjectManager()
    effective_stage = _get_effective_stage(project_id, from_location)

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Planning dcm project {project_id}", total=None)
        result = manager.plan(
            project_identifier=project_id,
            configuration=effective_configuration,
            from_stage=effective_stage,
            variables=variables,
            output_path=effective_output_path,
        )

    return QueryJsonValueResult(result)


@app.command(requires_connection=True)
def create(
    identifier: Optional[FQN] = optional_dcm_identifier,
    if_not_exists: bool = IfNotExistsOption(
        help="Do nothing if the project already exists."
    ),
    target: Optional[str] = target_option,
    **options,
):
    """
    Creates a DCM Project in Snowflake.
    """
    context = _resolve_target_context(identifier, target, use_config=False)
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
def list_deployments(
    identifier: Optional[FQN] = optional_dcm_identifier,
    target: Optional[str] = target_option,
    **options,
):
    """
    Lists deployments of given DCM Project.
    """
    context = _resolve_target_context(identifier, target, use_config=False)
    project_id = context.project_identifier

    pm = DCMProjectManager()
    results = pm.list_deployments(project_identifier=project_id)
    return QueryResult(results)


@app.command(requires_connection=True)
def drop_deployment(
    identifier: Optional[FQN] = optional_dcm_identifier,
    deployment_name: str = typer.Argument(
        help="Name or alias of the deployment to drop. For names containing '$', use single quotes to prevent shell expansion (e.g., 'DEPLOYMENT$1').",
        show_default=False,
    ),
    if_exists: bool = IfExistsOption(
        help="Do nothing if the deployment does not exist."
    ),
    target: Optional[str] = target_option,
    **options,
):
    """
    Drops a deployment from the DCM Project.
    """
    context = _resolve_target_context(identifier, target, use_config=False)
    project_id = context.project_identifier

    # Detect potential shell expansion issues
    if deployment_name and deployment_name.upper() == "DEPLOYMENT":
        cli_console.warning(
            f"Deployment name '{deployment_name}' might be truncated due to shell expansion. "
            f"If you meant to use a deployment like 'DEPLOYMENT$1', try using single quotes: 'DEPLOYMENT$1'."
        )

    dpm = DCMProjectManager()
    dpm.drop_deployment(
        project_identifier=project_id,
        deployment_name=deployment_name,
        if_exists=if_exists,
    )
    return MessageResult(
        f"Deployment '{deployment_name}' dropped from DCM Project '{project_id}'."
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
    from_location: Optional[str] = from_option,
    variables: Optional[List[str]] = variables_flag,
    configuration: Optional[str] = configuration_flag,
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
    context = _resolve_target_context(
        identifier, target, from_location, use_config=True, use_output_path=False
    )
    effective_configuration = configuration or context.configuration
    project_id = context.project_identifier

    manager = DCMProjectManager()
    effective_stage = _get_effective_stage(project_id, from_location)

    with cli_console.spinner() as spinner:
        spinner.add_task(
            description=f"Previewing {object_identifier}.",
            total=None,
        )
        result = manager.preview(
            project_identifier=project_id,
            object_identifier=object_identifier,
            configuration=effective_configuration,
            from_stage=effective_stage,
            variables=variables,
            limit=limit,
        )

    return QueryResult(result)


@app.command(requires_connection=True)
@mock_dcm_response("refresh")
def refresh(
    identifier: Optional[FQN] = optional_dcm_identifier,
    target: Optional[str] = target_option,
    **options,
):
    """
    Refreshes dynamic tables defined in DCM project.
    """
    context = _resolve_target_context(identifier, target, use_config=False)
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
    target: Optional[str] = target_option,
    **options,
):
    """
    Tests all expectations defined in DCM project.
    """
    context = _resolve_target_context(identifier, target, use_config=False)
    project_id = context.project_identifier

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Testing dcm project {project_id}", total=None)
        result = DCMProjectManager().test(project_identifier=project_id)

    reporter = TestReporter()
    reporter.process(result)
    return EmptyResult()


def _get_effective_stage(identifier: FQN, from_location: Optional[str]):
    manager = DCMProjectManager()
    if not from_location:
        from_stage = manager.sync_local_files(project_identifier=identifier)
    elif is_stage_path(from_location):
        from_stage = from_location
    else:
        from_stage = manager.sync_local_files(
            project_identifier=identifier, source_directory=from_location
        )
    return from_stage
