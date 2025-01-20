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
from typing import Dict

import typer
from click import UsageError
from snowflake.cli._plugins.notebook.manager import NotebookManager
from snowflake.cli._plugins.notebook.notebook_entity_model import NotebookEntityModel
from snowflake.cli._plugins.notebook.types import NotebookStagePath
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import (
    with_project_definition,
)
from snowflake.cli.api.commands.flags import (
    IfNotExistsOption,
    ReplaceOption,
    entity_argument,
    identifier_argument,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.entities.common import EntityActions
from snowflake.cli.api.exceptions import NoProjectDefinitionError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
)
from typing_extensions import Annotated

app = SnowTyperFactory(
    name="notebook",
    help="Manages notebooks in Snowflake.",
)
log = logging.getLogger(__name__)

NOTEBOOK_IDENTIFIER = identifier_argument(sf_object="notebook", example="MY_NOTEBOOK")
NotebookFile: NotebookStagePath = typer.Option(
    "--notebook-file",
    "-f",
    help="Stage path with notebook file. For example `@stage/path/to/notebook.ipynb`",
    show_default=False,
)


@app.command(requires_connection=True)
def execute(
    identifier: FQN = NOTEBOOK_IDENTIFIER,
    **options,
):
    """
    Executes a notebook in a headless mode.
    """
    # Execution does not return any meaningful result
    _ = NotebookManager().execute(notebook_name=identifier)
    return MessageResult(f"Notebook {identifier} executed.")


@app.command(requires_connection=True)
def get_url(
    identifier: FQN = NOTEBOOK_IDENTIFIER,
    **options,
):
    """Return a url to a notebook."""
    url = NotebookManager().get_url(notebook_name=identifier)
    return MessageResult(message=url)


@app.command(name="open", requires_connection=True)
def open_cmd(
    identifier: FQN = NOTEBOOK_IDENTIFIER,
    **options,
):
    """Opens a notebook in default browser"""
    url = NotebookManager().get_url(notebook_name=identifier)
    typer.launch(url)
    return MessageResult(message=url)


@app.command(requires_connection=True)
def create(
    identifier: Annotated[FQN, NOTEBOOK_IDENTIFIER],
    notebook_file: Annotated[NotebookStagePath, NotebookFile],
    **options,
):
    """Creates notebook from stage."""
    notebook_url = NotebookManager().create(
        notebook_name=identifier,
        notebook_file=notebook_file,
    )
    return MessageResult(message=notebook_url)


@app.command(requires_connection=True)
@with_project_definition()
def deploy(
    entity_id=entity_argument("notebook"),
    replace: bool = ReplaceOption(
        help="Replace existing Notebook if it already exists.",
    ),
    if_not_exists: bool = IfNotExistsOption(help="Skip if Notebook already exists."),
    **options,
) -> CommandResult:
    """Uploads a notebook to a stage and creates a notebook. If entity_id is not provided,
    deploys all notebooks defined in the project definition."""
    cli_context = get_cli_context()
    pd = cli_context.project_definition
    if not pd.meets_version_requirement("2"):
        raise UsageError(
            "This command requires project definition of version at least 2."
        )

    notebooks: Dict[str, NotebookEntityModel] = pd.get_entities_by_type(
        entity_type="notebook"
    )
    if not notebooks:
        raise NoProjectDefinitionError(
            project_type="notebook", project_root=cli_context.project_root
        )
    if entity_id:
        if entity_id not in notebooks:
            raise UsageError(f"No '{entity_id}' notebook in project definition file.")
        notebooks = {entity_id: notebooks[entity_id]}

    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )

    deploy_results = []
    for entity_id in sorted(notebooks):
        result = ws.perform_action(
            entity_id,
            EntityActions.DEPLOY,
            replace=replace,
            if_not_exists=if_not_exists,
        )
        deploy_results.append(result)

    return CollectionResult(deploy_results)
