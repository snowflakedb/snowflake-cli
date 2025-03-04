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

from typing import List, Optional

import typer
from snowflake.cli._plugins.project.feature_flags import FeatureFlag
from snowflake.cli._plugins.project.manager import ProjectManager
from snowflake.cli._plugins.project.project_entity_model import (
    ProjectEntityModel,
)
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.artifacts.upload import put_files
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import (
    entity_argument,
    identifier_argument,
    variables_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.commands.utils import get_entity_for_operation
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import MessageResult, SingleQueryResult
from snowflake.cli.api.project.project_paths import ProjectPaths

app = SnowTyperFactory(
    name="project",
    help="Manages projects in Snowflake.",
    is_hidden=FeatureFlag.ENABLE_SNOWFLAKE_PROJECTS.is_disabled,
)

project_identifier = identifier_argument(sf_object="project", example="MY_PROJECT")
version_flag = typer.Option(
    ..., "--version", help="Version of the project to use.", show_default=False
)
variables_flag = variables_option(
    'Variables for the execution context; for example: `-D "<key>=<value>"`.'
)


@app.command(requires_connection=True)
def execute(
    identifier: FQN = project_identifier,
    version: str = version_flag,
    variables: Optional[List[str]] = variables_flag,
    **options,
):
    """
    Executes a project.
    """
    result = ProjectManager().execute(
        project_name=identifier, version=version, variables=variables
    )
    return SingleQueryResult(result)


@app.command(requires_connection=True)
def dry_run(
    identifier: FQN = project_identifier,
    version: str = version_flag,
    variables: Optional[List[str]] = variables_flag,
    **options,
):
    """
    Validates a project.
    """
    result = ProjectManager().execute(
        project_name=identifier, version=version, dry_run=True, variables=variables
    )
    return SingleQueryResult(result)


@app.command(requires_connection=True)
@with_project_definition()
def create_version(
    entity_id: str = entity_argument("project"),
    **options,
):
    """
    Upload local files and create a new version of a project using those files. If the stage does not exist, it will be created.
    """
    cli_context = get_cli_context()
    project: ProjectEntityModel = get_entity_for_operation(
        cli_context=cli_context,
        entity_id=entity_id,
        project_definition=cli_context.project_definition,
        entity_type="project",
    )

    # Sync state
    with cli_console.phase("Syncing project state"):
        stage_name = FQN.from_stage(project.stage)
        sm = StageManager()

        cli_console.step(f"Creating stage {stage_name}")
        sm.create(fqn=stage_name)

        put_files(
            project_paths=ProjectPaths(project_root=cli_context.project_root),
            stage_root=project.stage,
            artifacts=project.artifacts,
        )

    # Create project and version
    with cli_console.phase("Creating project and version"):
        pm = ProjectManager()
        cli_console.step(f"Creating project {project.fqn}")
        pm.create(project_name=project.fqn)

        cli_console.step(f"Creating version from stage {stage_name}")
        pm.create_version(project_name=project.fqn, stage_name=stage_name)
    return MessageResult(f"Project {project.fqn} deployed.")


@app.command(requires_connection=True)
@with_project_definition()
def add_version(
    entity_id: str = entity_argument("project"),
    _from: str = typer.Option(
        ...,
        "--from",
        help="Source stage to create the version from.",
        show_default=False,
    ),
    alias: str
    | None = typer.Option(
        None, "--alias", help="Alias for the version.", show_default=False
    ),
    comment: str
    | None = typer.Option(
        None, "--comment", help="Version comment.", show_default=False
    ),
    **options,
):
    """Adds a new version to a project using existing sources from provided stage path."""

    pm = ProjectManager()
    pm.add_version(
        project_name=entity_id,
        from_stage=_from,
        alias=alias,
        comment=comment,
    )
    return MessageResult("Version added.")
