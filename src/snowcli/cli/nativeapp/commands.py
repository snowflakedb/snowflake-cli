from typing import Optional
import logging
import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.output.decorators import with_output, catch_error
from snowcli.output.printing import OutputData

from .init import nativeapp_init
from .manager import NativeAppManager
from .artifacts import ArtifactError

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    hidden=True,
    name="app",
    help="Manage Native Apps in Snowflake",
)

log = logging.getLogger(__name__)

ProjectArgument = typer.Option(
    None,
    "-p",
    "--project",
    help="Path where the Native Apps project resides. Defaults to current working directory",
    show_default=False,
)


@app.command("init")
@with_output
def app_init(
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
    nativeapp_init(name, template)
    return OutputData.from_string(
        f"Native Apps project {name} has been created in your local directory."
    )


@app.command("bundle", hidden=True)
@with_output
@catch_error(ArtifactError, exit_code=1)
def app_bundle(
    project_path: Optional[str] = ProjectArgument,
) -> OutputData:
    """
    Prepares a local folder with configured app artifacts.
    """
    manager = NativeAppManager(project_path)
    manager.build_bundle()
    return OutputData.from_string(f"Bundle generated at {manager.deploy_root}")
