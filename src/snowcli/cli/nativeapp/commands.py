from typing import Optional
from textwrap import dedent
from pathlib import Path

import logging
import typer

from snowcli.cli.common.decorators import global_options, global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.output.decorators import with_output
from snowcli.cli.stage.diff import DiffResult, stage_diff

from .init import nativeapp_init
from .manager import NativeAppManager

from snowcli.output.types import (
    CommandResult,
    MessageResult,
)

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
@global_options
def app_init(
    name: str = typer.Argument(
        ..., help="Name of the Native Apps project to be initiated."
    ),
    git_url: str = typer.Option(
        None,
        help="A git URL to use as template for the Native Apps project. Example: https://github.com/Snowflake-Labs/native-apps-templates.git for all official Snowflake templates.",
    ),
    template: str = typer.Option(
        None,
        help="A specific directory within the git URL to use as template for the Native Apps project. Example: Default is native-app-basic if --git-url is https://github.com/Snowflake-Labs/native-apps-templates.git, and None if any other --git-url.",
    ),
    **options,
) -> CommandResult:
    """
    Initialize a Native Apps project, optionally with a --git-url and a --template.
    """
    nativeapp_init(name, git_url, template)
    return MessageResult(
        f"Native Apps project {name} has been created in your local directory."
    )


@app.command("bundle", hidden=True)
@with_output
@global_options
def app_bundle(
    project_path: Optional[str] = ProjectArgument,
    **options,
) -> CommandResult:
    """
    Prepares a local folder with configured app artifacts.
    """
    manager = NativeAppManager(project_path)
    manager.build_bundle()
    return MessageResult(f"Bundle generated at {manager.deploy_root}")


@app.command("diff", hidden=True)
@with_output
@global_options_with_connection
def nativeapp_stage_diff(
    stage_fqn: str = typer.Argument(None, help="Name of stage"),
    folder_name: str = typer.Argument(None, help="Path to local folder"),
    **options,
) -> MessageResult:
    """
    Diffs a stage with a local folder
    """
    diff: DiffResult = stage_diff(Path(folder_name), stage_fqn)
    output = f"""\
        only local:  {', '.join(diff.only_local)}
        only stage:  {', '.join(diff.only_on_stage)}
        mod/unknown: {', '.join(diff.different)}
        identical:   {', '.join(diff.identical)}"""

    return MessageResult(dedent(output))
