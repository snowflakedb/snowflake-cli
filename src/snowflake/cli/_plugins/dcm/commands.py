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
from pathlib import Path
from typing import List, Optional

import typer
from snowflake.cli._plugins.dcm.debug import get_debug_cursor
from snowflake.cli._plugins.dcm.manager import AnalysisType, DCMProjectManager
from snowflake.cli._plugins.dcm.reporting import (
    AnalyzeReporter,
    DCMCommandResult,
    PlanReporter,
    RefreshReporter,
    TestReporter,
)
from snowflake.cli._plugins.dcm.utils import (
    TestResultFormat,
)
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
    MessageResult,
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
    identifier: FQN = dcm_identifier,
    from_location: Optional[str] = from_option,
    variables: Optional[List[str]] = variables_flag,
    configuration: Optional[str] = configuration_flag,
    alias: Optional[str] = alias_option,
    skip_plan: bool = typer.Option(
        False,
        "--skip-plan",
        help="Skips planning step",
    ),
    **options,
):
    """
    Applies changes defined in DCM Project to Snowflake.
    """
    # Check for debug mode
    debug_cursor = get_debug_cursor("deploy")
    if debug_cursor:
        return DCMCommandResult(debug_cursor, PlanReporter("deploy")).process()

    manager = DCMProjectManager()
    effective_stage = _get_effective_stage(identifier, from_location)

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Deploying dcm project {identifier}", total=None)
        if skip_plan:
            cli_console.warning("Skipping planning step")
        result = manager.deploy(
            project_identifier=identifier,
            configuration=configuration,
            from_stage=effective_stage,
            variables=variables,
            alias=alias,
            skip_plan=skip_plan,
        )

    return DCMCommandResult(result, PlanReporter("deploy")).process()


@app.command(requires_connection=True)
def plan(
    identifier: FQN = dcm_identifier,
    from_location: Optional[str] = from_option,
    variables: Optional[List[str]] = variables_flag,
    configuration: Optional[str] = configuration_flag,
    output_path: Optional[str] = output_path_option(
        help="Path where the deployment plan output will be stored. Can be a stage path (starting with '@') or a local directory path."
    ),
    **options,
):
    """
    Plans a DCM Project deployment (validates without executing).
    """
    # Check for debug mode
    debug_cursor = get_debug_cursor("plan")
    if debug_cursor:
        return DCMCommandResult(debug_cursor, PlanReporter("plan")).process()

    manager = DCMProjectManager()
    effective_stage = _get_effective_stage(identifier, from_location)

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Planning dcm project {identifier}", total=None)
        result = manager.plan(
            project_identifier=identifier,
            configuration=configuration,
            from_stage=effective_stage,
            variables=variables,
            output_path=output_path,
        )

    return DCMCommandResult(result, PlanReporter("plan")).process()


@app.command(requires_connection=True)
def create(
    identifier: FQN = dcm_identifier,
    if_not_exists: bool = IfNotExistsOption(
        help="Do nothing if the project already exists."
    ),
    **options,
):
    """
    Creates a DCM Project in Snowflake.
    """
    om = ObjectManager()
    if om.object_exists(object_type="dcm", fqn=identifier):
        message = f"DCM Project '{identifier}' already exists."
        if if_not_exists:
            return MessageResult(message)
        raise CliError(message)

    dpm = DCMProjectManager()
    with cli_console.phase(f"Creating DCM Project '{identifier}'"):
        dpm.create(project_identifier=identifier)

    return MessageResult(f"DCM Project '{identifier}' successfully created.")


@app.command(requires_connection=True)
def list_deployments(
    identifier: FQN = dcm_identifier,
    **options,
):
    """
    Lists deployments of given DCM Project.
    """
    pm = DCMProjectManager()
    results = pm.list_deployments(project_identifier=identifier)
    return QueryResult(results)


@app.command(requires_connection=True)
def drop_deployment(
    identifier: FQN = dcm_identifier,
    deployment_name: str = typer.Argument(
        help="Name or alias of the deployment to drop. For names containing '$', use single quotes to prevent shell expansion (e.g., 'DEPLOYMENT$1').",
        show_default=False,
    ),
    if_exists: bool = IfExistsOption(
        help="Do nothing if the deployment does not exist."
    ),
    **options,
):
    """
    Drops a deployment from the DCM Project.
    """
    # Detect potential shell expansion issues
    if deployment_name and deployment_name.upper() == "DEPLOYMENT":
        cli_console.warning(
            f"Deployment name '{deployment_name}' might be truncated due to shell expansion. "
            f"If you meant to use a deployment like 'DEPLOYMENT$1', try using single quotes: 'DEPLOYMENT$1'."
        )

    dpm = DCMProjectManager()
    dpm.drop_deployment(
        project_identifier=identifier,
        deployment_name=deployment_name,
        if_exists=if_exists,
    )
    return MessageResult(
        f"Deployment '{deployment_name}' dropped from DCM Project '{identifier}'."
    )


@app.command(requires_connection=True)
def test(
    identifier: FQN = dcm_identifier,
    export_format: Optional[List[TestResultFormat]] = typer.Option(
        None,
        "--result-format",
        help="Export test results in specified format(s) into directory set with `--output-path`. Can be specified multiple times for multiple formats.",
        show_default=False,
    ),
    output_path: Optional[Path] = typer.Option(
        None,
        "--output-path",
        help="Directory where test result files will be saved. Defaults to current directory.",
        show_default=False,
    ),
    **options,
):
    """
    Test all expectations set for tables, views and dynamic tables defined
    in DCM project.
    """
    # Check for debug mode
    debug_cursor = get_debug_cursor("test")
    if debug_cursor:
        return DCMCommandResult(debug_cursor, TestReporter()).process()

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Testing dcm project {identifier}", total=None)
        result = DCMProjectManager().test(project_identifier=identifier)

    # TODO: Integrate export_format and output_path into TestReporter
    # The export_test_results function (JUnit, TAP, JSON export) should be
    # integrated into the reporter's process() method or added as a post-process hook.
    # This would allow the reporter to handle all output formats consistently.
    # For now, export_format functionality is disabled during refactoring.

    return DCMCommandResult(result, TestReporter()).process()


@app.command(requires_connection=True)
def refresh(
    identifier: FQN = dcm_identifier,
    **options,
):
    """
    Refreshes dynamic tables defined in DCM project.
    """
    # Check for debug mode
    debug_cursor = get_debug_cursor("refresh")
    if debug_cursor:
        return DCMCommandResult(debug_cursor, RefreshReporter()).process()

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Refreshing dcm project {identifier}", total=None)
        result = DCMProjectManager().refresh(project_identifier=identifier)

    return DCMCommandResult(result, RefreshReporter()).process()


@app.command(requires_connection=True)
def preview(
    identifier: FQN = dcm_identifier,
    object_identifier: FQN = typer.Option(
        ...,
        "--object",
        help="FQN of table/view/etc to be previewed.",
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
    **options,
):
    """
    Returns rows from any table, view, dynamic table.

    Examples:
    \nsnow dcm preview MY_PROJECT --configuration DEV --object MY_DB.PUBLIC.MY_VIEW --limit 2
    """
    manager = DCMProjectManager()
    effective_stage = _get_effective_stage(identifier, from_location)

    with cli_console.spinner() as spinner:
        spinner.add_task(
            description=f"Previewing {object_identifier}.",
            total=None,
        )
        result = manager.preview(
            project_identifier=identifier,
            object_identifier=object_identifier,
            configuration=configuration,
            from_stage=effective_stage,
            variables=variables,
            limit=limit,
        )

    return QueryResult(result)


@app.command(requires_connection=True)
def analyze(
    identifier: FQN = dcm_identifier,
    from_location: Optional[str] = from_option,
    files: Optional[List[Path]] = typer.Option(
        None,
        "--files",
        help="Files to analyze. Can be specified multiple times for multiple files.",
        show_default=False,
    ),
    variables: Optional[List[str]] = variables_flag,
    configuration: Optional[str] = configuration_flag,
    analysis_type: Optional[AnalysisType] = typer.Option(
        None,
        "--type",
        help="Type of analysis to perform.",
        show_default=False,
        case_sensitive=False,
    ),
    output_path: Optional[str] = output_path_option(
        help="Path where the analysis result will be stored. Can be a stage path (starting with '@') or a local directory path."
    ),
    **options,
):
    """
    Analyzes a DCM Project.
    """
    # Check for debug mode
    debug_cursor = get_debug_cursor("analyze")
    if debug_cursor:
        return DCMCommandResult(debug_cursor, AnalyzeReporter()).process()

    manager = DCMProjectManager()
    effective_stage = _get_effective_stage(identifier, from_location, files)

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Analyzing dcm project {identifier}", total=None)
        result = manager.analyze(
            project_identifier=identifier,
            configuration=configuration,
            from_stage=effective_stage,
            variables=variables,
            analysis_type=analysis_type,
            output_path=output_path,
            files=files,
        )

    return DCMCommandResult(result, AnalyzeReporter()).process()


def _get_effective_stage(
    identifier: FQN, from_location: Optional[str], files: Optional[List[Path]] = None
):
    manager = DCMProjectManager()
    if not from_location:
        from_stage = manager.sync_local_files(
            project_identifier=identifier, files=files
        )
    elif is_stage_path(from_location):
        from_stage = from_location
    else:
        from_stage = manager.sync_local_files(
            project_identifier=identifier, source_directory=from_location, files=files
        )
    return from_stage
