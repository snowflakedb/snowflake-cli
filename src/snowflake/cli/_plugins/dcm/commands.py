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
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import typer
from snowflake.cli._plugins.dcm.manager import AnalysisType
from snowflake.cli._plugins.dcm.manager import DCMProjectManager
from snowflake.cli._plugins.dcm.utils import (
    TestResultFormat,
    export_test_results,
    format_refresh_results,
    format_test_failures,
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
    return QueryJsonValueResult(result)


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

    return QueryJsonValueResult(result)


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
    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Testing dcm project {identifier}", total=None)
        result = DCMProjectManager().test(project_identifier=identifier)

    row = result.fetchone()
    if not row:
        return MessageResult("No data.")

    result_data = row[0]
    result_json = (
        json.loads(result_data) if isinstance(result_data, str) else result_data
    )

    expectations = result_json.get("expectations", [])

    if not expectations:
        return MessageResult("No expectations defined in the project.")

    if export_format:
        if output_path is None:
            output_path = Path().cwd()
        saved_files = export_test_results(result_json, export_format, output_path)
        if saved_files:
            cli_console.step(f"Test results exported to {output_path.resolve()}.")

    if result_json.get("status") == "EXPECTATION_VIOLATED":
        failed_expectations = [
            exp for exp in expectations if exp.get("expectation_violated", False)
        ]
        total_tests = len(expectations)
        failed_count = len(failed_expectations)
        error_message = format_test_failures(
            failed_expectations, total_tests, failed_count
        )
        raise CliError(error_message)

    return MessageResult(f"All {len(expectations)} expectation(s) passed successfully.")


@app.command(requires_connection=True)
def refresh(
    identifier: FQN = dcm_identifier,
    **options,
):
    """
    Refreshes dynamic tables defined in DCM project.
    """
    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Refreshing dcm project {identifier}", total=None)
        result = DCMProjectManager().refresh(project_identifier=identifier)

    row = result.fetchone()
    if not row:
        return MessageResult("No data.")

    result_data = row[0]
    result_json = (
        json.loads(result_data) if isinstance(result_data, str) else result_data
    )

    refreshed_tables = result_json.get("refreshed_tables", [])
    message = format_refresh_results(refreshed_tables)

    return MessageResult(message)


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
    manager = DCMProjectManager()
    effective_stage = _get_effective_stage(identifier, from_location)

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Analyzing dcm project {identifier}", total=None)
        result = manager.analyze(
            project_identifier=identifier,
            configuration=configuration,
            from_stage=effective_stage,
            variables=variables,
            analysis_type=analysis_type,
            output_path=output_path,
        )

    row = result.fetchone()
    if not row:
        return MessageResult("No data.")

    result_data = row[0]
    result_json = (
        json.loads(result_data) if isinstance(result_data, str) else result_data
    )

    summary = _analyze_result_summary(result_json)

    if summary.has_errors:
        error_message = _format_error_message(summary)
        raise CliError(error_message)

    return MessageResult(
        f"✓ Analysis complete: {summary.total_files} file(s) analyzed, "
        f"{summary.total_definitions} definition(s) found. No errors detected."
    )


@dataclass
class AnalysisSummary:
    total_files: int = 0
    total_definitions: int = 0
    files_with_errors: int = 0
    total_errors: int = 0
    errors_by_file: Dict[str, List[str]] = field(default_factory=dict)
    has_errors: bool = False


def _analyze_result_summary(result_json) -> AnalysisSummary:
    summary = AnalysisSummary()

    if not isinstance(result_json, dict):
        return summary

    files = result_json.get("files", [])
    summary.total_files = len(files)

    for file_info in files:
        source_path = file_info.get("sourcePath", "unknown")
        file_errors = []

        # Check file-level errors
        for error in file_info.get("errors", []):
            error_msg = error.get("message", "Unknown error")
            file_errors.append(error_msg)
            summary.total_errors += 1

        # Check definition-level errors
        definitions = file_info.get("definitions", [])
        summary.total_definitions += len(definitions)

        for definition in definitions:
            for error in definition.get("errors", []):
                error_msg = error.get("message", "Unknown error")
                file_errors.append(error_msg)
                summary.total_errors += 1

        if file_errors:
            summary.errors_by_file[source_path] = file_errors
            summary.files_with_errors += 1
            summary.has_errors = True

    return summary


def _format_error_message(summary: AnalysisSummary) -> str:
    lines = [
        f"Analysis found {summary.total_errors} error(s) in {summary.files_with_errors} file(s):",
        "",
    ]

    for file_path, errors in summary.errors_by_file.items():
        lines.append(f"  {file_path}:")
        for error in errors:
            lines.append(f"    • {error}")
        lines.append("")

    return "\n".join(lines).rstrip()


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
