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
from snowflake.cli._plugins.object.command_aliases import add_object_command_aliases
from snowflake.cli._plugins.object.commands import scope_option
from snowflake.cli._plugins.project.feature_flags import FeatureFlag
from snowflake.cli._plugins.project.manager import ProjectManager
from snowflake.cli._plugins.project.project_entity_model import (
    ProjectEntityModel,
)
from snowflake.cli.api.artifacts.upload import sync_artifacts_with_stage
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import (
    OverrideableOption,
    PruneOption,
    entity_argument,
    identifier_argument,
    like_option,
    variables_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.commands.utils import get_entity_for_operation
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import MessageResult, QueryResult, SingleQueryResult
from snowflake.cli.api.project.project_paths import ProjectPaths

app = SnowTyperFactory(
    name="project",
    help="Manages projects in Snowflake.",
    is_hidden=FeatureFlag.ENABLE_SNOWFLAKE_PROJECTS.is_disabled,
)

project_identifier = identifier_argument(sf_object="project", example="MY_PROJECT")
version_flag = typer.Option(
    None,
    "--version",
    help="Version of the project to use. If not specified default version is used",
    show_default=False,
)
variables_flag = variables_option(
    'Variables for the execution context; for example: `-D "<key>=<value>"`.'
)
no_version_flag = OverrideableOption(
    False,
    "--no-version",
    help="Do not initialize project with a new version, only create the snowflake object.",
)
from_option = OverrideableOption(
    None,
    "--from",
    help="Create a new version using given stage instead of uploading local files.",
    show_default=False,
)


add_object_command_aliases(
    app=app,
    object_type=ObjectType.PROJECT,
    name_argument=project_identifier,
    like_option=like_option(
        help_example='`list --like "my%"` lists all projects that begin with “my”'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
    ommit_commands=["drop", "create", "describe"],
)


@app.command(requires_connection=True)
def execute(
    identifier: FQN = project_identifier,
    version: Optional[str] = version_flag,
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
    version: Optional[str] = version_flag,
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


def _add_version_to_project(
    pm: ProjectManager,
    project: ProjectEntityModel,
    prune: bool = False,
    from_stage: Optional[str] = None,
    alias: Optional[str] = None,
    comment: Optional[str] = None,
):
    """
    Adds a version to project. If [from_stage] is not defined,
    uploads local files to the stage defined in project definition.
    """

    if not from_stage:
        cli_context = get_cli_context()
        from_stage = project.stage
        with cli_console.phase("Uploading artifacts"):
            sync_artifacts_with_stage(
                project_paths=ProjectPaths(project_root=cli_context.project_root),
                stage_root=from_stage,
                artifacts=project.artifacts,
                prune=prune,
            )

    with cli_console.phase(f"Creating project version from stage {from_stage}"):
        return pm.add_version(
            project_name=project.fqn,
            from_stage=from_stage,
            alias=alias,
            comment=comment,
        )


@app.command(requires_connection=True)
@with_project_definition()
def create(
    entity_id: str = entity_argument("project"),
    no_version: bool = no_version_flag(
        mutually_exclusive=["prune"],
    ),
    prune: bool = PruneOption(mutually_exclusive=["no_version"]),
    **options,
):
    """
    Creates a project in snowflake and initializes it with a new version created from local files.
    """
    cli_context = get_cli_context()
    project: ProjectEntityModel = get_entity_for_operation(
        cli_context=cli_context,
        entity_id=entity_id,
        project_definition=cli_context.project_definition,
        entity_type="project",
    )
    pm = ProjectManager()
    with cli_console.phase("Creating project (if not exists)"):
        result = pm.create(project.fqn)

    if no_version:
        return QueryResult(result)

    _add_version_to_project(pm, project=project, prune=prune)
    return MessageResult(
        f"Project {project.fqn} successfully created and initial version is added."
    )


@app.command(requires_connection=True)
@with_project_definition()
def add_version(
    entity_id: str = entity_argument("project"),
    _from: Optional[str] = from_option(mutually_exclusive=["prune"]),
    _alias: Optional[str] = typer.Option(
        None, "--alias", help="Alias for the version.", show_default=False
    ),
    comment: Optional[str] = typer.Option(
        None, "--comment", help="Version comment.", show_default=False
    ),
    prune: bool = PruneOption(mutually_exclusive=["_from"]),
    **options,
):
    """Uploads local files to Snowflake and cerates a new project version."""
    cli_context = get_cli_context()
    project: ProjectEntityModel = get_entity_for_operation(
        cli_context=cli_context,
        entity_id=entity_id,
        project_definition=cli_context.project_definition,
        entity_type="project",
    )
    _add_version_to_project(
        ProjectManager(),
        project=project,
        prune=prune,
        from_stage=_from,
        alias=_alias,
        comment=comment,
    )
    alias_str = "" if _alias is None else _alias + " "
    return MessageResult(f"Project version '{alias_str}' added to project {project.fqn}")


@app.command(requires_connection=True)
def list_versions(
    entity_id: str = entity_argument("project", required=True), **options
):
    """
    Lists versions of given project.
    """
    pm = ProjectManager()
    results = pm.list_versions(project_name=FQN.from_string(entity_id))
    return QueryResult(results)
