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

import logging

import typer
from snowflake.cli._plugins.project.manager import ProjectManager
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import entity_argument, identifier_argument
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.commands.utils import get_entity_for_operation
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.api.project.schemas.entities.project_entity_model import (
    ProjectEntityModel,
)

app = SnowTyperFactory(
    name="project",
    help="Manages projects in Snowflake.",
)
log = logging.getLogger(__name__)

project_identifier = identifier_argument(sf_object="project", example="MY_PROJECT")
version_flag = typer.Option("--version", help="Version of the project to use.")


@app.command(requires_connection=True)
def execute(
    identifier: FQN = project_identifier,
    version: str = version_flag,
    **options,
):
    """
    Executes a project.
    """
    result = ProjectManager().execute(project_name=identifier, version=version)
    return result


@app.command(requires_connection=True)
def validate(
    identifier: FQN = project_identifier,
    version: str = version_flag,
    **options,
):
    """
    Validates a project.
    """
    result = ProjectManager().execute(
        project_name=identifier, version=version, dry_run=True
    )
    return result


@app.command(requires_connection=True)
@with_project_definition()
def create_version(
    entity_id: str = entity_argument("project"),
    **options,
):
    """
    Create a new version of a project. This command will create a new version of the project
    in the specified stage. If the stage does not exist, it will be created.
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

        # TODO: improve this behavior
        for file in project.artifacts:
            cli_console.step(f"Uploading {file} to {stage_name}")
            if isinstance(file, str):
                sm.put(local_path=file, stage_path=stage_name)
            else:
                sm.put(
                    local_path=file.local,
                    stage_path=file.remote,
                )

    # Create project and version
    with cli_console.phase("Creating project and version"):
        pm = ProjectManager()
        cli_console.step(f"Creating project {project.fqn}")
        pm.create(project_name=project.fqn)

        cli_console.step(f"Creating version from stage {stage_name}")
        pm.create_version(project_name=project.fqn, stage_name=stage_name)
    return MessageResult(f"Project {project.fqn} deployed.")
