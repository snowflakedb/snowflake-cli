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
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.project.feature_flags import FeatureFlag
from snowflake.cli._plugins.project.manager import ProjectManager
from snowflake.cli._plugins.project.project_entity_model import (
    ProjectEntityModel,
)
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
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import MessageResult, QueryResult, SingleQueryResult

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
    ommit_commands=["create", "describe"],
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


@app.command(requires_connection=True)
@with_project_definition()
def create(
    entity_id: str = entity_argument("project"),
    no_version: bool = typer.Option(
        False,
        "--no-version",
        help="Do not initialize project with a new version, only create the snowflake object.",
    ),
    **options,
):
    """
    Creates a project in Snowflake.
    By default, the project is initialized with a new version created from local files.
    """
    cli_context = get_cli_context()
    project: ProjectEntityModel = get_entity_for_operation(
        cli_context=cli_context,
        entity_id=entity_id,
        project_definition=cli_context.project_definition,
        entity_type="project",
    )
    om = ObjectManager()
    if om.object_exists(object_type="project", fqn=project.fqn):
        raise CliError(f"Project '{project.fqn}' already exists.")
    if not no_version and om.object_exists(
        object_type="stage", fqn=FQN.from_stage(project.stage)
    ):
        raise CliError(f"Stage '{project.stage}' already exists.")

    pm = ProjectManager()
    with cli_console.phase(f"Creating project '{project.fqn}'"):
        pm.create(project=project, initialize_version_from_local_files=not no_version)

    if no_version:
        return MessageResult(f"Project '{project.fqn}' successfully created.")
    return MessageResult(
        f"Project '{project.fqn}' successfully created and initial version is added."
    )


@app.command(requires_connection=True)
@with_project_definition()
def add_version(
    entity_id: str = entity_argument("project"),
    _from: Optional[str] = from_option(),
    _alias: Optional[str] = typer.Option(
        None, "--alias", help="Alias for the version.", show_default=False
    ),
    comment: Optional[str] = typer.Option(
        None, "--comment", help="Version comment.", show_default=False
    ),
    prune: bool = PruneOption(default=True),
    **options,
):
    """Uploads local files to Snowflake and cerates a new project version."""
    if _from is not None and prune:
        cli_console.warning(
            "When `--from` option is used, `--prune` option will be ignored and files from stage will be used as they are."
        )
        prune = False
    cli_context = get_cli_context()
    project: ProjectEntityModel = get_entity_for_operation(
        cli_context=cli_context,
        entity_id=entity_id,
        project_definition=cli_context.project_definition,
        entity_type="project",
    )
    om = ObjectManager()
    if not om.object_exists(object_type="project", fqn=project.fqn):
        raise CliError(
            f"Project '{project.fqn}' does not exist. Use `project create` command first"
        )
    ProjectManager().add_version(
        project=project,
        prune=prune,
        from_stage=_from,
        alias=_alias,
        comment=comment,
    )
    alias_str = "" if _alias is None else f"'{_alias}' "
    return MessageResult(
        f"New project version {alias_str}added to project '{project.fqn}'"
    )


@app.command(requires_connection=True)
def list_versions(
    identifier: FQN = project_identifier,
    **options,
):
    """
    Lists versions of given project.
    """
    pm = ProjectManager()
    results = pm.list_versions(project_name=identifier)
    return QueryResult(results)
