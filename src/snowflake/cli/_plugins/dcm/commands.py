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
import time
from typing import List, Optional

import typer
from snowflake.cli._plugins.dcm.manager import DCMProjectManager
from snowflake.cli._plugins.object.command_aliases import add_object_command_aliases
from snowflake.cli._plugins.object.commands import scope_option
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.artifacts.upload import sync_artifacts_with_stage
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.flags import (
    IfExistsOption,
    IfNotExistsOption,
    OverrideableOption,
    identifier_argument,
    like_option,
    variables_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.commands.utils import get_entity_for_operation
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    MessageResult,
    QueryJsonValueResult,
    QueryResult,
)
from snowflake.cli.api.project.project_paths import ProjectPaths
from snowflake.cli.api.project.util import unquote_identifier

MANIFEST_FILE_NAME = "manifest.yml"

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
from_option = OverrideableOption(
    None,
    "--from",
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
    from_stage: Optional[str] = from_option(
        help="Deploy DCM Project deployment from a given stage."
    ),
    variables: Optional[List[str]] = variables_flag,
    configuration: Optional[str] = configuration_flag,
    alias: Optional[str] = alias_option,
    **options,
):
    """
    Applies changes defined in DCM Project to Snowflake.
    """
    result = DCMProjectManager().execute(
        project_identifier=identifier,
        configuration=configuration,
        from_stage=from_stage if from_stage else _sync_local_files(),
        variables=variables,
        alias=alias,
        output_path=None,
    )
    return QueryJsonValueResult(result)


@app.command(requires_connection=True)
def plan(
    identifier: FQN = dcm_identifier,
    from_stage: Optional[str] = from_option(
        help="Plan DCM Project deployment from a given stage."
    ),
    variables: Optional[List[str]] = variables_flag,
    configuration: Optional[str] = configuration_flag,
    output_path: Optional[str] = output_path_option(
        help="Stage path where the deployment plan output will be stored."
    ),
    **options,
):
    """
    Plans a DCM Project deployment (validates without executing).
    """
    result = DCMProjectManager().execute(
        project_identifier=identifier,
        configuration=configuration,
        from_stage=from_stage if from_stage else _sync_local_files(),
        dry_run=True,
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


def _sync_local_files() -> str:
    cli_context = get_cli_context()
    project_entity = get_entity_for_operation(
        cli_context=cli_context,
        entity_id=None,
        project_definition=cli_context.project_definition,
        entity_type="dcm",
    )

    # Create a temporary stage for this deployment session
    stage_manager = StageManager()
    unquoted_name = unquote_identifier(project_entity.fqn.name)
    stage_fqn = FQN.from_string(
        f"DCM_{unquoted_name}_{int(time.time())}_TMP_STAGE"
    ).using_context()

    with cli_console.phase("Creating temporary stage for deployment"):
        stage_manager.create(fqn=stage_fqn, temporary=True)
        cli_console.step(f"Created temporary stage: {stage_fqn}")

    with cli_console.phase("Syncing local files to temporary stage"):
        sync_artifacts_with_stage(
            project_paths=ProjectPaths(project_root=cli_context.project_root),
            stage_root=stage_fqn.identifier,
            artifacts=project_entity.artifacts,
        )

    return stage_fqn.identifier
