import logging
import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.nativeapp.manager import NativeAppManager
from snowcli.output.decorators import with_output
from snowcli.output.printing import OutputData

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    hidden=True,
    name="app",
    help="Manage Native Apps in Snowflake",
)

log = logging.getLogger(__name__)


@app.command("init")
@with_output
def nativeapp_init(
    name: str = typer.Argument(
        ..., help="Name of the Native Apps project to be initiated."
    ),
    template: str = typer.Option(
        None, help="A git URL to use as template for the Native Apps project."
    ),
) -> OutputData:
    """
    Initialize a Native Apps project, optionally with a --template.
    """

    NativeAppManager().nativeapp_init(name, template)
    return OutputData().from_string(
        f"Native Apps project {name} has been created in your local directory."
    )
