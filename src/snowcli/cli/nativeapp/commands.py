from typing import Optional
import logging
import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.output.decorators import with_output
from snowcli.output.printing import OutputData

from .manager import NativeAppManager

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    hidden=False,
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

    pass


@app.command("bundle")
@with_output
def nativeapp_bundle(
    project_path: Optional[str] = ProjectArgument,
) -> OutputData:
    try:
        manager = NativeAppManager(project_path)
        manager.build_bundle()
        return OutputData.from_string(f"Bundle generated at {manager.deploy_root}")

    except Exception as e:
        return OutputData.from_string(str(e)).add_exit_code(1)
