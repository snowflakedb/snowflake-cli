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

import click
import typer
from click import ClickException
from snowflake.cli._plugins.object.command_aliases import (
    add_object_command_aliases,
    scope_option,
)
from snowflake.cli._plugins.streamlit.manager import StreamlitManager
from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import (
    with_experimental_behaviour,
    with_project_definition,
)
from snowflake.cli.api.commands.flags import (
    PruneOption,
    ReplaceOption,
    entity_argument,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.commands.utils import get_entity_for_operation
from snowflake.cli.api.console.console import CliConsole
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.entities.utils import EntityActions
from snowflake.cli.api.exceptions import NoProjectDefinitionError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.definition_conversion import (
    convert_project_definition_to_v2,
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


@app.command(requires_connection=True)
def execute(
    name: FQN = StreamlitNameArgument,
    **options,
):
    """
    Executes a streamlit in a headless mode.
    """
    _ = StreamlitManager().execute(app_name=name)
    return MessageResult(f"Streamlit {name} executed.")


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
        help="Replaces the Streamlit app if it already exists. It only uploads new and overwrites existing files, "
        "but does not remove any files already on the stage."
    ),
    prune: bool = PruneOption(),
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
        pd = convert_project_definition_to_v2(cli_context.project_root, pd)

    streamlit: StreamlitEntity = StreamlitEntity(
        entity_model=get_entity_for_operation(
            cli_context=cli_context,
            entity_id=entity_id,
            project_definition=pd,
            entity_type=ObjectType.STREAMLIT.value.cli_name,
        ),
        workspace_ctx=_get_current_workspace_context(),
    )

    url = streamlit.perform(
        EntityActions.DEPLOY,
        ActionContext(
            get_entity=lambda *args: None,
        ),
        _open=open_,
        replace=replace,
        experimental=options.get("experimental"),
        prune=prune,
    )

    if open_:
        typer.launch(url)

    return MessageResult(f"Streamlit successfully deployed and available under {url}")


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


def _get_current_workspace_context():
    ctx = get_cli_context()

    return WorkspaceContext(
        console=CliConsole(),
        project_root=ctx.project_root,
        get_default_role=lambda: ctx.connection.role,
        get_default_warehouse=lambda: ctx.connection.warehouse,
    )
