import logging

import typer
from snowflake.cli.api.commands.flags import identifier_argument
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.plugins.notebook.manager import NotebookManager

app = SnowTyper(
    name="notebook",
    help="Manages notebooks in Snowflake.",
    hidden=FeatureFlag.ENABLE_NOTEBOOKS.is_disabled(),
)
log = logging.getLogger(__name__)

NOTEBOOK_IDENTIFIER = identifier_argument(sf_object="notebook", example="MY_NOTEBOOK")


@app.command(requires_connection=True)
def execute(
    identifier: str = NOTEBOOK_IDENTIFIER,
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
    identifier: str = NOTEBOOK_IDENTIFIER,
    **options,
):
    """Return a url to a notebook."""
    url = NotebookManager().get_url(notebook_name=identifier)
    return MessageResult(message=url)


@app.command(name="open", requires_connection=True)
def open_cmd(
    identifier: str = NOTEBOOK_IDENTIFIER,
    **options,
):
    """Opens a notebook in default browser"""
    url = NotebookManager().get_url(notebook_name=identifier)
    typer.launch(url)
    return MessageResult(message=url)
