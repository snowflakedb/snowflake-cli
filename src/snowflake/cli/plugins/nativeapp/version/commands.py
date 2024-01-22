import logging
from typing import Optional

import typer
from click import MissingParameter
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.decorators import (
    global_options_with_connection,
    with_output,
    with_project_definition,
)
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS
from snowflake.cli.api.output.types import CommandResult, MessageResult, QueryResult
from snowflake.cli.plugins.nativeapp.common_flags import ForceOption, InteractiveOption
from snowflake.cli.plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
)
from snowflake.cli.plugins.nativeapp.run_processor import NativeAppRunProcessor
from snowflake.cli.plugins.nativeapp.utils import is_tty_interactive
from snowflake.cli.plugins.nativeapp.version.version_processor import (
    NativeAppVersionCreateProcessor,
    NativeAppVersionDropProcessor,
)

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="version",
    help="Manage Native Application Package versions in Snowflake",
)

log = logging.getLogger(__name__)


@app.command()
@with_output
@with_project_definition("native_app")
@global_options_with_connection
def create(
    version: Optional[str] = typer.Argument(
        None,
        help=f"""Version of the app package for which you want to a version or patch. Defaults to the version specified in the `manifest.yml` file.""",
    ),
    patch: Optional[str] = typer.Option(
        None,
        "--patch",
        help=f"""The patch number you want to create for an existing version.
        Defaults to undefined if it is not set, which means the CLI either uses the patch specified in the `manifest.yml` file or automatically generates a new patch number.""",
    ),
    skip_git_check: Optional[bool] = typer.Option(
        False,
        "--skip-git-check",
        help="When enabled, the CLI skips checking if your project has any untracked or stages files in git. Default: unset.",
        is_flag=True,
    ),
    interactive: Optional[bool] = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """
    Adds a new patch to the provided version for your application package. If the version does not exist, creates a version with patch 0.
    """
    if version is None and patch is not None:
        raise MissingParameter("Cannot provide a patch without version!")

    is_interactive = False
    if force:
        policy = AllowAlwaysPolicy()
    elif interactive or is_tty_interactive():
        is_interactive = True
        policy = AskAlwaysPolicy()
    else:
        policy = DenyAlwaysPolicy()

    if skip_git_check:
        git_policy = DenyAlwaysPolicy()
    else:
        git_policy = AllowAlwaysPolicy()

    processor = NativeAppVersionCreateProcessor(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )

    # We need build_bundle() to (optionally) find version in manifest.yml and create app package
    processor.build_bundle()
    processor.process(
        version=version,
        patch=patch,
        policy=policy,
        git_policy=git_policy,
        is_interactive=is_interactive,
    )
    return MessageResult(f"Version create is now complete.")


@app.command("list")
@with_output
@with_project_definition("native_app")
@global_options_with_connection
def version_list(
    **options,
) -> CommandResult:
    """
    Lists all versions available in an application package.
    """
    processor = NativeAppRunProcessor(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    cursor = processor.get_all_existing_versions()
    return QueryResult(cursor)


@app.command()
@with_output
@with_project_definition("native_app")
@global_options_with_connection
def drop(
    version: Optional[str] = typer.Argument(
        None,
        help="Version of the app package that you want to drop. Defaults to the version specified in the `manifest.yml` file.",
    ),
    interactive: Optional[bool] = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """
    Drops a version associated with your application package. Version can either be passed in as an argument to the command or read from the `manifest.yml` file.
    Dropping patches is not allowed.
    """
    is_interactive = False
    if force:
        policy = AllowAlwaysPolicy()
    elif interactive or is_tty_interactive():
        is_interactive = True
        policy = AskAlwaysPolicy()
    else:
        policy = DenyAlwaysPolicy()

    processor = NativeAppVersionDropProcessor(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    processor.process(version, policy, is_interactive)
    return MessageResult(f"Version drop is now complete.")
