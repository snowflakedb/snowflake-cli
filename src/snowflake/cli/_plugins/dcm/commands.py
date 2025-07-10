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
from snowflake.cli._plugins.dcm.dcm_project_entity_model import (
    DCMProjectEntityModel,
)
from snowflake.cli._plugins.dcm.manager import DCMProjectManager
from snowflake.cli._plugins.object.command_aliases import add_object_command_aliases
from snowflake.cli._plugins.object.commands import scope_option
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import (
    IfExistsOption,
    IfNotExistsOption,
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
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    MessageResult,
    QueryJsonValueResult,
    QueryResult,
)

app = SnowTyperFactory(
    name="dcm",
    help="Manages DCM Projects in Snowflake.",
    is_hidden=FeatureFlag.ENABLE_SNOWFLAKE_PROJECTS.is_disabled,
)

dcm_identifier = identifier_argument(sf_object="DCM Project", example="MY_PROJECT")
version_flag = typer.Option(
    None,
    "--version",
    help="Version of the DCM Project to use. If not specified default version is used. For names containing '$', use single quotes to prevent shell expansion (e.g., 'VERSION$1').",
    show_default=False,
)
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


add_object_command_aliases(
    app=app,
    object_type=ObjectType.DCM_PROJECT,
    name_argument=dcm_identifier,
    like_option=like_option(
        help_example='`list --like "my%"` lists all DCM Projects that begin with "my"'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
    ommit_commands=["create", "describe"],
)


@app.command(requires_connection=True)
def execute(
    identifier: FQN = dcm_identifier,
    version: Optional[str] = version_flag,
    from_stage: Optional[str] = from_option(
        help="Execute DCM Project from given stage instead of using a specific version."
    ),
    variables: Optional[List[str]] = variables_flag,
    configuration: Optional[str] = configuration_flag,
    **options,
):
    """
    Executes a DCM Project.
    """
    if version and from_stage:
        raise CliError("--version and --from are mutually exclusive.")

    result = DCMProjectManager().execute(
        project_name=identifier,
        configuration=configuration,
        version=version,
        from_stage=from_stage,
        variables=variables,
    )
    return QueryJsonValueResult(result)


@app.command(requires_connection=True)
def dry_run(
    identifier: FQN = dcm_identifier,
    version: Optional[str] = version_flag,
    from_stage: Optional[str] = from_option(
        help="Execute DCM Project from given stage instead of using a specific version."
    ),
    variables: Optional[List[str]] = variables_flag,
    configuration: Optional[str] = configuration_flag,
    **options,
):
    """
    Validates a DCM Project.
    """
    if version and from_stage:
        raise CliError("--version and --from are mutually exclusive.")

    result = DCMProjectManager().execute(
        project_name=identifier,
        configuration=configuration,
        version=version,
        from_stage=from_stage,
        dry_run=True,
        variables=variables,
    )
    return QueryJsonValueResult(result)


@app.command(requires_connection=True)
@with_project_definition()
def create(
    entity_id: str = entity_argument("dcm"),
    no_version: bool = typer.Option(
        False,
        "--no-version",
        help="Do not initialize DCM Project with a new version, only create the snowflake object.",
    ),
    if_not_exists: bool = IfNotExistsOption(
        help="Do nothing if the project already exists."
    ),
    **options,
):
    """
    Creates a DCM Project in Snowflake.
    By default, the DCM Project is initialized with a new version created from local files.
    """
    cli_context = get_cli_context()
    project: DCMProjectEntityModel = get_entity_for_operation(
        cli_context=cli_context,
        entity_id=entity_id,
        project_definition=cli_context.project_definition,
        entity_type="dcm",
    )
    om = ObjectManager()
    if om.object_exists(object_type="dcm", fqn=project.fqn):
        message = f"DCM Project '{project.fqn}' already exists."
        if if_not_exists:
            return MessageResult(message)
        raise CliError(message)

    if not no_version and om.object_exists(
        object_type="stage", fqn=FQN.from_stage(project.stage)
    ):
        raise CliError(f"Stage '{project.stage}' already exists.")

    dpm = DCMProjectManager()
    with cli_console.phase(f"Creating DCM Project '{project.fqn}'"):
        dpm.create(project=project, initialize_version_from_local_files=not no_version)

    if no_version:
        return MessageResult(f"DCM Project '{project.fqn}' successfully created.")
    return MessageResult(
        f"DCM Project '{project.fqn}' successfully created and initial version is added."
    )


@app.command(requires_connection=True)
@with_project_definition()
def add_version(
    entity_id: str = entity_argument("dcm"),
    _from: Optional[str] = from_option(
        help="Create a new version using given stage instead of uploading local files."
    ),
    _alias: Optional[str] = typer.Option(
        None, "--alias", help="Alias for the version.", show_default=False
    ),
    comment: Optional[str] = typer.Option(
        None, "--comment", help="Version comment.", show_default=False
    ),
    prune: bool = PruneOption(default=True),
    **options,
):
    """Uploads local files to Snowflake and cerates a new DCM Project version."""
    if _from is not None and prune:
        cli_console.warning(
            "When `--from` option is used, `--prune` option will be ignored and files from stage will be used as they are."
        )
        prune = False
    cli_context = get_cli_context()
    project: DCMProjectEntityModel = get_entity_for_operation(
        cli_context=cli_context,
        entity_id=entity_id,
        project_definition=cli_context.project_definition,
        entity_type="dcm",
    )
    om = ObjectManager()
    if not om.object_exists(object_type="dcm", fqn=project.fqn):
        raise CliError(
            f"DCM Project '{project.fqn}' does not exist. Use `dcm create` command first."
        )
    DCMProjectManager().add_version(
        project=project,
        prune=prune,
        from_stage=_from,
        alias=_alias,
        comment=comment,
    )
    alias_str = "" if _alias is None else f"'{_alias}' "
    return MessageResult(
        f"New version {alias_str}added to DCM Project '{project.fqn}'."
    )


@app.command(requires_connection=True)
def list_versions(
    identifier: FQN = dcm_identifier,
    **options,
):
    """
    Lists versions of given DCM Project.
    """
    pm = DCMProjectManager()
    results = pm.list_versions(project_name=identifier)
    return QueryResult(results)


@app.command(requires_connection=True)
def drop_version(
    identifier: FQN = dcm_identifier,
    version_name: str = typer.Argument(
        help="Name or alias of the version to drop. For names containing '$', use single quotes to prevent shell expansion (e.g., 'VERSION$1').",
        show_default=False,
    ),
    if_exists: bool = IfExistsOption(help="Do nothing if the version does not exist."),
    **options,
):
    """
    Drops a version from the DCM Project.
    """
    # Detect potential shell expansion issues
    if version_name and version_name.upper() == "VERSION":
        cli_console.warning(
            f"Version name '{version_name}' might be truncated due to shell expansion. "
            f"If you meant to use a version like 'VERSION$1', try using single quotes: 'VERSION$1'."
        )

    dpm = DCMProjectManager()
    dpm.drop_version(
        project_name=identifier,
        version_name=version_name,
        if_exists=if_exists,
    )
    return MessageResult(
        f"Version '{version_name}' dropped from DCM Project '{identifier}'."
    )
