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
from click import UsageError
from snowflake.cli._plugins.notebook.manager import NotebookManager
from snowflake.cli._plugins.notebook.notebook_entity_model import NotebookEntityModel
from snowflake.cli._plugins.notebook.types import NotebookStagePath
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.command_docs import (
    PUBLIC_PREVIEW_NO_GOV,
    AdmonitionType,
    CommandDocs,
    Example,
    admonition,
    bullet,
    bullet_list,
    code,
    link,
    plain_text,
    ref,
)
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import (
    PruneOption,
    ReplaceOption,
    entity_argument,
    identifier_argument,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.commands.utils import get_entity_for_operation
from snowflake.cli.api.entities.common import EntityActions
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
)
from typing_extensions import Annotated

app = SnowTyperFactory(
    name="notebook",
    help="Manages notebooks in Snowflake.",
)
log = logging.getLogger(__name__)

_NOTEBOOK_RELATED = (
    link("/developer-guide/snowflake-cli/index"),
    link("/developer-guide/snowflake-cli/command-reference/notebook-commands/overview"),
)

_NOTEBOOK_FQN_REQUIREMENTS = bullet_list(
    bullet("The notebook must already be deployed."),
    bullet(
        "If your notebook is running under a different database and schema than "
        "specified in the connection, you must provide them in name as a "
        "fully-qualified name, such as ",
        code("database.schema.name"),
        ".",
    ),
)

NOTEBOOK_IDENTIFIER = identifier_argument(sf_object="notebook", example="MY_NOTEBOOK")
NotebookFile: NotebookStagePath = typer.Option(
    "--notebook-file",
    "-f",
    help="Stage path with notebook file. For example `@stage/path/to/notebook.ipynb`",
    show_default=False,
)


@app.command(
    requires_connection=True,
    docs=CommandDocs(
        related=_NOTEBOOK_RELATED,
        usage_notes=(
            plain_text(
                "The ",
                code("snow notebook execute"),
                " command executes a notebook in headless mode. Currently, the "
                "command only returns a message indicating whether the notebook "
                "executed successfully. It doesn't return any result data from the "
                "notebook.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "The following example executes the ",
                    code("MY_NOTEBOOK"),
                    " notebook:",
                ),
                command="snow notebook execute MY_NOTEBOOK",
                output="Notebook MY_NOTEBOOK executed.",
            ),
        ),
    ),
)
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


@app.command(
    requires_connection=True,
    docs=CommandDocs(
        related=_NOTEBOOK_RELATED,
        usage_notes=(
            plain_text(
                "The notebook get-url command returns a url link to an existing "
                "notebooks. Note the following requirements:",
            ),
            _NOTEBOOK_FQN_REQUIREMENTS,
        ),
        examples=(
            Example(
                description=plain_text(
                    "This example gets a URL for an notebook using a fully-qualified "
                    "database and schema name:",
                ),
                command="snow notebook get-url database.schema.my_notebook",
            ),
        ),
    ),
)
def get_url(
    identifier: FQN = NOTEBOOK_IDENTIFIER,
    **options,
):
    """Return a url to a notebook."""
    url = NotebookManager().get_url(notebook_name=identifier)
    return MessageResult(message=url)


@app.command(
    name="open",
    requires_connection=True,
    docs=CommandDocs(
        related=_NOTEBOOK_RELATED,
        usage_notes=(
            plain_text(
                "The notebook open command opens existing notebooks in your default "
                "browser. Note the following requirements:",
            ),
            _NOTEBOOK_FQN_REQUIREMENTS,
        ),
        examples=(
            Example(
                description=plain_text(
                    "This example opens a notebook using a fully-qualified database "
                    "and schema name:",
                ),
                command="snow notebook open database.schema.my_notebook",
            ),
        ),
    ),
)
def open_cmd(
    identifier: FQN = NOTEBOOK_IDENTIFIER,
    **options,
):
    """Opens a notebook in default browser"""
    url = NotebookManager().get_url(notebook_name=identifier)
    typer.launch(url)
    return MessageResult(message=url)


@app.command(
    requires_connection=True,
    docs=CommandDocs(
        related=_NOTEBOOK_RELATED,
        banners=(
            admonition(
                AdmonitionType.NOTE,
                "Beginning with version 3.4.0, ",
                ref("sf-cli"),
                " added the ",
                link(
                    "/developer-guide/snowflake-cli/command-reference/notebook-commands/deploy"
                ),
                " command to replace the ",
                code("snow notebook create"),
                " command. To support backward compatibility, you can still create "
                "a notebook using this command, but Snowflake recommends that you "
                "begin using the new ",
                link("#label-cli-deploy-notebook"),
                " procedure.",
            ),
        ),
        usage_notes=(
            plain_text(
                "By default, the command creates notebooks using the default warehouse "
                "provided in the connection. You can use the ",
                code("--warehouse"),
                " parameter to specify a different warehouse or to specify one if the "
                "connection does not include a warehouse.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "The following example creates ",
                    code("MY_NOTEBOOK"),
                    " from the staged ",
                    code("@MY_STAGE/path/to/notebook.ipynb"),
                    " notebook:",
                ),
                command=(
                    "snow notebook create MY_NOTEBOOK -f @MY_STAGE/path/to/notebook.ipynb"
                ),
            ),
        ),
    ),
)
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


@app.command(
    requires_connection=True,
    docs=CommandDocs(
        related=_NOTEBOOK_RELATED,
        banners=(PUBLIC_PREVIEW_NO_GOV,),
        usage_notes=(
            plain_text(
                "The ",
                code("snow notebook deploy"),
                " command uploads local files to a stage and creates a new Notebook "
                "object inside your chosen database and schema. Your ",
                link("#label-cli-deploy-notebook", "project definition file"),
                " should specify the main notebook file and query warehouse. The ",
                code("--replace"),
                " option replaces the specified Notebook object if it already exists.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "The following example uploads the files specified in your "
                    "project definition file and creates a new notebook named ",
                    code("my_notebook"),
                    ":",
                ),
                command="snow notebook deploy my_notebook",
                output=(
                    "Uploading artifacts to @notebooks/my_notebook\n"
                    "  Creating stage notebooks if not exists\n"
                    "  Uploading artifacts\n"
                    "Creating notebook my_notebook\n"
                    "Notebook successfully deployed and available under "
                    "https://snowflake.com/provider-deduced-from-connection/#/notebooks/DB.SCHEMA.MY_NOTEBOOK"
                ),
            ),
        ),
    ),
)
@with_project_definition()
def deploy(
    entity_id: str = entity_argument("notebook"),
    replace: bool = ReplaceOption(
        help="Replace notebook object if it already exists. It only uploads new and overwrites existing files, "
        "but does not remove any files already on the stage.",
    ),
    prune: bool = PruneOption(),
    **options,
) -> CommandResult:
    """Uploads a notebook and required files to a stage and creates a Snowflake notebook."""
    cli_context = get_cli_context()
    pd = cli_context.project_definition
    if not pd.meets_version_requirement("2"):
        raise UsageError(
            "This command requires project definition of version at least 2."
        )

    notebook: NotebookEntityModel = get_entity_for_operation(
        cli_context=cli_context,
        entity_id=entity_id,
        project_definition=pd,
        entity_type="notebook",
    )
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    notebook_url = ws.perform_action(
        notebook.entity_id,
        EntityActions.DEPLOY,
        replace=replace,
        prune=prune,
    )
    return MessageResult(
        f"Notebook successfully deployed and available under {notebook_url}"
    )
