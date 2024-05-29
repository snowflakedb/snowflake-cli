import logging

import typer
from snowflake.cli.api.commands.flags import identifier_argument
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.plugins.notebook.manager import NotebookManager
from snowflake.cli.plugins.notebook.types import NotebookName, NotebookStagePath
from typing_extensions import Annotated

log = logging.getLogger(__name__)

NOTEBOOK_IDENTIFIER = identifier_argument(sf_object="notebook", example="MY_NOTEBOOK")

NotebookFile: NotebookStagePath = typer.Option(
    "--notebook-file",
    "-f",
    help="Stage path with notebook file. For example `@stage/path/to/notebook.ipynb`",
)


class SnowTyperCreator:
    def __init__(self):
        self.commands_to_register = []

    def create_snow_typer_instance(self, **kwargs) -> SnowTyper:
        app = SnowTyper(**kwargs)
        for command, cmd_args, cmd_kwargs in self.commands_to_register:
            app.command(*cmd_args, **cmd_kwargs)(command)
        return app

    def command(self, *args, **kwargs):
        def decorator(foo):
            self.commands_to_register.append((foo, args, kwargs))

        return decorator


creator = SnowTyperCreator()


@creator.command(requires_connection=True)
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


@creator.command(requires_connection=True)
def get_url(
    identifier: str = NOTEBOOK_IDENTIFIER,
    **options,
):
    """Return a url to a notebook."""
    url = NotebookManager().get_url(notebook_name=identifier)
    return MessageResult(message=url)


@creator.command(name="open", requires_connection=True)
def open_cmd(
    identifier: str = NOTEBOOK_IDENTIFIER,
    **options,
):
    """Opens a notebook in default browser"""
    url = NotebookManager().get_url(notebook_name=identifier)
    typer.launch(url)
    return MessageResult(message=url)


@creator.command(requires_connection=True)
def create(
    identifier: Annotated[NotebookName, NOTEBOOK_IDENTIFIER],
    notebook_file: Annotated[NotebookStagePath, NotebookFile],
    **options,
):
    """Creates notebook from stage."""
    notebook_url = NotebookManager().create(
        notebook_name=identifier,
        notebook_file=notebook_file,
    )
    return MessageResult(message=notebook_url)


def create_app():
    return creator.create_snow_typer_instance(
        name="notebook",
        help="Manages notebooks in Snowflake.",
        hidden=FeatureFlag.ENABLE_NOTEBOOKS.is_disabled(),
    )
