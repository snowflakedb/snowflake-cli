import logging
from typing import Optional

import typer
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.decorators import (
    global_options,
    global_options_with_connection,
    with_output,
    with_project_definition,
)
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.plugins.nativeapp.common_flags import ForceOption, InteractiveOption
from snowflake.cli.plugins.nativeapp.init import nativeapp_init
from snowflake.cli.plugins.nativeapp.manager import NativeAppManager
from snowflake.cli.plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
)
from snowflake.cli.plugins.nativeapp.run_processor import NativeAppRunProcessor
from snowflake.cli.plugins.nativeapp.teardown_processor import (
    NativeAppTeardownProcessor,
)
from snowflake.cli.plugins.nativeapp.utils import is_tty_interactive
from snowflake.cli.plugins.nativeapp.version.commands import app as versions_app

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
        help=f"""Directory to be initialized with the Native Application project. This directory must not already exist.""",
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
        help=f"""The identifier or version name of the version of an existing application package from which you want to create an application instance.
        The application and application package names are determined from the project definition file.""",
    ),
    patch: Optional[str] = typer.Option(
        None,
        "--patch",
        help=f"""The patch number under the given `--version` of an existing application package that should be used to create an application instance.
        The application and application package names are determined from the project definition file.""",
    ),
    from_release_directive: Optional[bool] = typer.Option(
        False,
        "--from-release-directive",
        help=f"""Creates or upgrades an application to the version and patch specified by the release directive applicable to your Snowflake account.
        The command fails if no release directive exists for your Snowflake account for a given application package, which is determined from the project definition file. Default: unset.""",
        is_flag=True,
    ),
    interactive: Optional[bool] = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """
    Creates an application package in your Snowflake account, uploads code files to its stage,
    then creates (or upgrades) a development-mode instance of that application.
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
    Opens the application inside of your browser,
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
    """
    processor = NativeAppTeardownProcessor(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    processor.process(force)
    return MessageResult(f"Teardown is now complete.")
