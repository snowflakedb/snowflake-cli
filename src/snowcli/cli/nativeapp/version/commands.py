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
from snowcli.cli.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
)
from snowcli.cli.nativeapp.run_processor import NativeAppRunProcessor
from snowcli.cli.nativeapp.utils import is_tty_interactive
from snowcli.cli.nativeapp.version.version_processor import (
    NativeAppVersionCreateProcessor,
    NativeAppVersionDropProcessor,
)
from snowcli.output.decorators import with_output
from snowcli.output.types import CommandResult, MessageResult, QueryResult

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
        help=f"""The identifier or 'version string' of the version you would like to create a version and/or patch for.
        Defaults to undefined, which means the CLI will use the version, if present, in the manifest.yml.""",
    ),
    patch: Optional[str] = typer.Option(
        None,
        "--patch",
        help=f"""The patch number you would like to create for an existing version.
        Defaults to undefined if it is not set, which means the CLI will either use the version, if present, in the manifest.yml,
        or auto-generate the patch number.""",
    ),
    interactive: Optional[bool] = typer.Option(
        False,
        "--interactive",
        "-i",
        help=f"""Defaults to unset. If specified, enables user interactions even if the standard input and output are not terminal devices.""",
        is_flag=True,
    ),
    force: Optional[bool] = typer.Option(
        False,
        "--force",
        help=f"""Defaults to unset. Passing in --force turns this to True, i.e. we will implicitly respond “yes” to any prompts that come up.
        This flag should be passed in if you are not in an interactive mode and want the command to succeed.""",
        is_flag=True,
    ),
    skip_git_check: Optional[bool] = typer.Option(
        False,
        "--skip-git-check",
        help="Defaults to unset. Passing in --skip-git-check turns this to True, i.e. we will skip checking if your project has any untracked or stages files in git.",
        is_flag=True,
    ),
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
    List all versions available in an application package.
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
        help="Version of the app package that you would like to drop. Defaults to the version specified in the manifest.yml.",
    ),
    interactive: Optional[bool] = typer.Option(
        False,
        "--interactive",
        "-i",
        help=f"""Defaults to unset. If specified, enables user interactions even if the standard input and output are not terminal devices.""",
        is_flag=True,
    ),
    force: Optional[bool] = typer.Option(
        False,
        "--force",
        help=f"""Defaults to unset. Passing in --force turns this to True, i.e. we will implicitly respond “yes” to any prompts that come up.
        This flag should be passed in if you are not in an interactive mode and want the command to succeed.""",
        is_flag=True,
    ),
    **options,
) -> CommandResult:
    """
    Drops a version associated with your application package. Version can either be passed in as an argument to the command or read from the manifest.yml file.
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
