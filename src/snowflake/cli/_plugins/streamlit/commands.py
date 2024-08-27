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

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import click
import typer
from click import ClickException, UsageError
from snowflake.cli._plugins.object.command_aliases import (
    add_object_command_aliases,
    scope_option,
)
from snowflake.cli._plugins.streamlit.manager import StreamlitManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import (
    with_experimental_behaviour,
    with_project_definition,
)
from snowflake.cli.api.commands.flags import (
    ReplaceOption,
    entity_argument,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import NoProjectDefinitionError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.schemas.entities.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectDefinition,
    ProjectDefinitionV2,
)

app = SnowTyperFactory(
    name="streamlit",
    help="Manages a Streamlit app in Snowflake.",
)
log = logging.getLogger(__name__)

StreamlitNameArgument = identifier_argument(
    sf_object="Streamlit app", example="my_streamlit"
)
OpenOption = typer.Option(
    False,
    "--open",
    help="Whether to open the Streamlit app in a browser.",
    is_flag=True,
)


add_object_command_aliases(
    app=app,
    object_type=ObjectType.STREAMLIT,
    name_argument=StreamlitNameArgument,
    like_option=like_option(
        help_example='`list --like "my%"` lists all streamlit apps that begin with “my”'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
)


@app.command("share", requires_connection=True)
def streamlit_share(
    name: FQN = StreamlitNameArgument,
    to_role: str = typer.Argument(
        ...,
        help="Role with which to share the Streamlit app.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """
    Shares a Streamlit app with another role.
    """
    cursor = StreamlitManager().share(streamlit_name=name, to_role=to_role)
    return SingleQueryResult(cursor)


def _default_file_callback(param_name: str):
    from click.core import ParameterSource  # type: ignore

    def _check_file_exists_if_not_default(ctx: click.Context, value):
        if (
            ctx.get_parameter_source(param_name) != ParameterSource.DEFAULT  # type: ignore
            and value
            and not Path(value).exists()
        ):
            raise ClickException(f"Provided file {value} does not exist")
        return Path(value)

    return _check_file_exists_if_not_default


@app.command("deploy", requires_connection=True)
@with_project_definition()
@with_experimental_behaviour()
def streamlit_deploy(
    replace: bool = ReplaceOption(
        help="Replace the Streamlit app if it already exists."
    ),
    entity_id: str = entity_argument("streamlit"),
    open_: bool = OpenOption,
    **options,
) -> CommandResult:
    """
    Deploys a Streamlit app defined in the project definition file (snowflake.yml). By default, the command uploads
    environment.yml and any other pages or folders, if present. If you don’t specify a stage name, the `streamlit`
    stage is used. If the specified stage does not exist, the command creates it. If multiple Streamlits are defined
    in snowflake.yml and no entity_id is provided then command will raise an error.
    """

    cli_context = get_cli_context()
    pd = cli_context.project_definition
    if not pd.meets_version_requirement("2"):
        if not pd.streamlit:
            raise NoProjectDefinitionError(
                project_type="streamlit", project_root=cli_context.project_root
            )
        pd = migrate_v1_streamlit_to_v2(pd)

    streamlits: Dict[str, StreamlitEntityModel] = pd.get_entities_by_type(
        entity_type="streamlit"
    )

    if not streamlits:
        raise NoProjectDefinitionError(
            project_type="streamlit", project_root=cli_context.project_root
        )

    if entity_id and entity_id not in streamlits:
        raise UsageError(f"No '{entity_id}' entity in project definition file.")

    if len(streamlits.keys()) == 1:
        entity_id = list(streamlits.keys())[0]

    if entity_id is None:
        raise UsageError(
            "Multiple Streamlit apps found. Please provide entity id for the operation."
        )

    # Get first streamlit
    streamlit: StreamlitEntityModel = streamlits[entity_id]
    url = StreamlitManager().deploy(streamlit=streamlit, replace=replace)

    if open_:
        typer.launch(url)

    return MessageResult(f"Streamlit successfully deployed and available under {url}")


def migrate_v1_streamlit_to_v2(pd: ProjectDefinition):
    default_env_file = "environment.yml"
    default_pages_dir = "pages"

    # Process env file
    environment_file = pd.streamlit.env_file
    if environment_file and not Path(environment_file).exists():
        raise ClickException(f"Provided file {environment_file} does not exist")
    elif environment_file is None and Path(default_env_file).exists():
        environment_file = default_env_file
    # Process pages dir
    pages_dir = pd.streamlit.pages_dir
    if pages_dir and not Path(pages_dir).exists():
        raise ClickException(f"Provided file {pages_dir} does not exist")
    elif pages_dir is None and Path(default_pages_dir).exists():
        pages_dir = default_pages_dir

    # Build V2 definition
    artifacts = [
        pd.streamlit.main_file,
        environment_file,
        pages_dir,
    ]
    artifacts = [a for a in artifacts if a is not None]
    if pd.streamlit.additional_source_files:
        artifacts.extend(pd.streamlit.additional_source_files)

    identifier = {"name": pd.streamlit.name}
    if pd.streamlit.schema_name:
        identifier["schema"] = pd.streamlit.schema_name
    if pd.streamlit.database:
        identifier["database"] = pd.streamlit.database

    if pd.streamlit.name.startswith("<%") and pd.streamlit.name.endswith("%>"):
        streamlit_name = "streamlit_entity_1"
    else:
        streamlit_name = pd.streamlit.name

    data = {
        "definition_version": "2",
        "entities": {
            streamlit_name: {
                "type": "streamlit",
                "identifier": identifier,
                "title": pd.streamlit.title,
                "query_warehouse": pd.streamlit.query_warehouse,
                "main_file": str(pd.streamlit.main_file),
                "pages_dir": str(pd.streamlit.pages_dir),
                "stage": pd.streamlit.stage,
                "artifacts": artifacts,
            }
        },
    }
    if hasattr(pd, "env") and pd.env:
        data["env"] = {k: v for k, v in pd.env.items()}
    return ProjectDefinitionV2(**data)


@app.command("get-url", requires_connection=True)
def get_url(
    name: FQN = StreamlitNameArgument,
    open_: bool = OpenOption,
    **options,
):
    """Returns a URL to the specified Streamlit app"""
    url = StreamlitManager().get_url(streamlit_name=name)
    if open_:
        typer.launch(url)
    return MessageResult(url)
