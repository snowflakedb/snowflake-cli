import logging
from typing import Optional

import typer
from click import MissingParameter
from snowcli.cli.common.cli_global_context import cli_context
from snowcli.cli.common.decorators import (
    global_options_with_connection,
    with_project_definition,
)
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.nativeapp.policy import AllowAlwaysPolicy, AlwaysAskPolicy
from snowcli.cli.nativeapp.version.version_processor import (
    NativeAppVersionCreateProcessor,
    NativeAppVersionDropProcessor,
)
from snowcli.output.decorators import with_output
from snowcli.output.types import CommandResult, MessageResult

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="version",
    help="Manage Native App Pkg versions in Snowflake",
)

log = logging.getLogger(__name__)


@app.command()
@with_output
@global_options_with_connection
def create(
    version: Optional[str] = typer.Argument(
        None,
        help=f"""The identifier or 'version string' of the version you would like to create a version and/or patch for.
        Defaults to undefined, which means the CLI will use the version, if present, in the manifest.yml.""",
    ),
    patch: Optional[str] = typer.Option(
        None,
        "--patch",
        "-p",
        help=f"""The patch number you would like to create for an existing version.
        Defaults to undefined if it is not set, which means the CLI will either use the version, if present, in the manifest.yml,
        or auto-generate the patch number.""",
    ),
    force: Optional[bool] = typer.Option(
        False,
        "--force",
        help="Defaults to False. Passing in --force turns this to True, i.e. we will implicitly respond “yes” to any prompts that come up.",
        is_flag=True,
    ),
    **options,
) -> CommandResult:
    """
    Adds a new patch to the provided version for your application package. If the version does not exist, creates a version with patch 0.
    """
    if version is None and patch is not None:
        raise MissingParameter("Cannot provide a patch without version!")

    if force:
        policy = AllowAlwaysPolicy()
    else:
        policy = AlwaysAskPolicy()

    processor = NativeAppVersionCreateProcessor(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    processor.process(version, patch, policy)
    return MessageResult(f"Version create is now complete.")


@app.command()
@with_output
@with_project_definition("native_app")
@global_options_with_connection
def drop(
    version: Optional[str] = typer.Argument(
        None,
        help="Version of the app package that you would like to drop. Defaults to the version specified in the manifest.yml.",
    ),
    force: Optional[bool] = typer.Option(
        False,
        "--force",
        help="Defaults to False. Passing in --force turns this to True, i.e. we will implicitly respond “yes” to any prompts that come up.",
        is_flag=True,
    ),
    **options,
) -> CommandResult:
    """
    Drops a version associated with your application package. Version can either be passed in as an argument to the command or read from the manifest.yml file.
    Dropping patches is not allowed.
    """
    if force:
        policy = AllowAlwaysPolicy()
    else:
        policy = AlwaysAskPolicy()

    processor = NativeAppVersionDropProcessor(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    processor.process(version, policy)
    return MessageResult(f"Version drop is now complete.")
