from typing import Optional

import logging
import typer

from snowcli.cli.common.decorators import (
    global_options_with_connection,
    global_options,
)
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.output.decorators import with_output

from snowcli.cli.nativeapp.init import nativeapp_init
from snowcli.cli.nativeapp.manager import NativeAppManager

from snowcli.output.types import CommandResult, MessageResult

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
    template_repo: str = typer.Option(
        None,
        help=f"""A git URL to a template repository, which can be a template itself or contain many templates inside it.
        Example: https://github.com/Snowflake-Labs/native-apps-templates.git for all official Snowflake templates.
        If using a private Github repo, you may be prompted to enter your Github username and password.
        Please use your personal access token in the password prompt, and refer to
        https://docs.github.com/en/get-started/getting-started-with-git/about-remote-repositories#cloning-with-https-urls for information on currently recommended modes of authentication.""",
    ),
    template: str = typer.Option(
        None,
        help="A specific template name within the template repo to use as template for the Native Apps project. Example: Default is basic if --template-repo is https://github.com/Snowflake-Labs/native-apps-templates.git, and None if any other --template-repo is specified.",
    ),
    **options,
) -> CommandResult:
    """
    Initialize a Native Apps project, optionally with a --template-repo and a --template.
    """
    nativeapp_init(name, template_repo, template)
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


@app.command("run")
@with_output
@global_options_with_connection
def app_run(
    project_path: Optional[str] = ProjectArgument,
    **options,
) -> CommandResult:
    """
    Creates an application package in your Snowflake account, uploads code files to its stage,
    then creates (or upgrades) a development-mode instance of that application. As a note, this
    command does not accept role or warehouse overrides to your config.toml file, because your
    native app definition in snowflake.yml/snowflake.local.yml is used for any overrides.
    """
    manager = NativeAppManager(project_path)
    manager.build_bundle()
    manager.app_run()
    return MessageResult(
        f'Your application ("{manager.app_name}") is now live:\n'
        + manager.get_snowsight_url()
    )


@app.command("open")
@with_output
@global_options_with_connection
def app_open(
    project_path: Optional[str] = ProjectArgument,
    **options,
) -> CommandResult:
    """
    Opens the (development mode) application inside of your browser,
    once it has been installed in your account.
    """
    manager = NativeAppManager(project_path)
    if manager.app_exists():
        typer.launch(manager.get_snowsight_url())
        return MessageResult(f"Application opened in browser.")
    else:
        return MessageResult(
            'Application not yet deployed! Please run "snow app run" first.'
        )


@app.command("teardown")
@with_output
@global_options_with_connection
def app_teardown(
    project_path: Optional[str] = ProjectArgument,
    **options,
) -> CommandResult:
    """
    Drops an application and an application package as defined in the project definition file.
    As a note, this command does not accept role or warehouse overrides to your config.toml file,
    because your native app definition in snowflake.yml/snowflake.local.yml is used for any overrides.
    """
    manager = NativeAppManager(project_path)
    manager.teardown()
    return MessageResult(f"Teardown is now complete.")
