import logging
from typing import Optional

import typer
from snowcli.api.output.decorators import with_output
from snowcli.api.output.types import CommandResult, MessageResult
from snowcli.cli.common.cli_global_context import cli_context
from snowcli.cli.common.decorators import (
    global_options,
    global_options_with_connection,
    with_project_definition,
)
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.nativeapp.common_flags import ForceOption, InteractiveOption
from snowcli.cli.nativeapp.init import nativeapp_init
from snowcli.cli.nativeapp.manager import NativeAppManager
from snowcli.cli.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
)
from snowcli.cli.nativeapp.run_processor import NativeAppRunProcessor
from snowcli.cli.nativeapp.teardown_processor import NativeAppTeardownProcessor
from snowcli.cli.nativeapp.utils import is_tty_interactive
from snowcli.cli.nativeapp.version.commands import app as versions_app

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="app",
    help="Manage Native Apps in Snowflake",
)
app.add_typer(versions_app)

log = logging.getLogger(__name__)


@app.command("init")
@with_output
@global_options
def app_init(
    path: str = typer.Argument(
        ...,
        help=f"""Directory to be initialized with the Native Application project. This directory must not already
        exist.""",
    ),
    name: str = typer.Option(
        None,
        help=f"""The name of the native application project to include in snowflake.yml. When not specified, it is
        generated from the name of the directory. Names are assumed to be unquoted identifiers whenever possible, but
        can be forced to be quoted by including the surrounding quote characters in the provided value.""",
    ),
    template_repo: str = typer.Option(
        None,
        help=f"""Specifies the git URL to a template repository, which can be a template itself or contain many templates inside it,
        such as https://github.com/snowflakedb/native-apps-templates.git for all official Snowflake templates.
        If using a private Github repo, you might be prompted to enter your Github username and password.
        Please use your personal access token in the password prompt, and refer to
        https://docs.github.com/en/get-started/getting-started-with-git/about-remote-repositories#cloning-with-https-urls for information on currently recommended modes of authentication.""",
    ),
    template: str = typer.Option(
        None,
        help="A specific template name within the template repo to use as template for the Native Apps project. Example: Default is basic if `--template-repo` is https://github.com/snowflakedb/native-apps-templates.git, and None if any other --template-repo is specified.",
    ),
    **options,
) -> CommandResult:
    """
    Initializes a Native Apps project.
    """
    project = nativeapp_init(
        path=path, name=name, git_url=template_repo, template=template
    )
    return MessageResult(
        f"Native Apps project {project.name} has been created at: {path}"
    )


@app.command("bundle", hidden=True)
@with_output
@with_project_definition("native_app")
@global_options
def app_bundle(
    **options,
) -> CommandResult:
    """
    Prepares a local folder with configured app artifacts.
    """
    manager = NativeAppManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    manager.build_bundle()
    return MessageResult(f"Bundle generated at {manager.deploy_root}")


@app.command("run")
@with_output
@with_project_definition("native_app")
@global_options_with_connection
def app_run(
    version: Optional[str] = typer.Option(
        None,
        help=f"""The identifier or 'version string' of the version you would like to create a version and/or patch for.
        If not specified, the version from manifest.yml will be used.""",
    ),
    patch: Optional[str] = typer.Option(
        None,
        "--patch",
        help=f"""The patch number you would like to create for an existing version.
        If not specified, the patch from manifest.yml will be used if it is present in the file. Otherwise, Snowflake will auto-generate the patch number.""",
    ),
    from_release_directive: Optional[bool] = typer.Option(
        False,
        "--from-release-directive",
        help=f"""Passing in this flag will upgrade the application if necessary to the version pointed to by the relevant release directive. Using this flag will fail if no such release directive exists.""",
        is_flag=True,
    ),
    interactive: Optional[bool] = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """
    Without any flags, this command creates an application package in your Snowflake account, uploads code files to its stage,
    then creates (or upgrades) a development-mode instance of that application.
    If passed in the version, patch or release directive flags, this command upgrades your existing application instance, or creates one if none exists. It does not create an application package in this scenario.
    """

    is_interactive = False
    if force:
        policy = AllowAlwaysPolicy()
    elif interactive or is_tty_interactive():
        is_interactive = True
        policy = AskAlwaysPolicy()
    else:
        policy = DenyAlwaysPolicy()

    processor = NativeAppRunProcessor(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    processor.build_bundle()
    processor.process(
        policy=policy,
        version=version,
        patch=patch,
        from_release_directive=from_release_directive,
        is_interactive=is_interactive,
    )
    return MessageResult(
        f"Your application ({processor.app_name}) is now live:\n"
        + processor.get_snowsight_url()
    )


@app.command("open")
@with_output
@with_project_definition("native_app")
@global_options_with_connection
def app_open(
    **options,
) -> CommandResult:
    """
    Opens the (development mode) application inside of your browser,
    once it has been installed in your account.
    """
    manager = NativeAppManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    if manager.get_existing_app_info():
        typer.launch(manager.get_snowsight_url())
        return MessageResult(f"Application opened in browser.")
    else:
        return MessageResult(
            'Application not yet deployed! Please run "snow app run" first.'
        )


@app.command("teardown")
@with_output
@with_project_definition("native_app")
@global_options_with_connection
def app_teardown(
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """
    Attempts to drop both the application and package as defined in the project definition file.
    This command will succeed even if one or both of these objects do not exist.
    As a note, this command does not accept role or warehouse overrides to your `config.toml` file,
    because your native app definition in `snowflake.yml/snowflake.local.yml` is used for any overrides.
    """
    processor = NativeAppTeardownProcessor(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    processor.process(force)
    return MessageResult(f"Teardown is now complete.")
