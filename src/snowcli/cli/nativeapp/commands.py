import logging
from typing import Optional

import typer
from snowcli.cli.common.cli_global_context import cli_context
from snowcli.cli.common.decorators import (
    global_options,
    global_options_with_connection,
    with_project_definition,
)
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.nativeapp.init import nativeapp_init
from snowcli.cli.nativeapp.manager import NativeAppManager
from snowcli.cli.nativeapp.run_processor import NativeAppRunProcessor
from snowcli.cli.nativeapp.teardown_processor import NativeAppTeardownProcessor
from snowcli.cli.nativeapp.version.commands import app as versions_app
from snowcli.output.decorators import with_output
from snowcli.output.types import CommandResult, MessageResult

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
    **options,
) -> CommandResult:
    """
    Creates an application package in your Snowflake account, uploads code files to its stage,
    then creates (or upgrades) a development-mode instance of that application. As a note, this
    command does not accept role or warehouse overrides to your `config.toml` file, because your
    native app definition in `snowflake.yml` or `snowflake.local.yml` is used for any overrides.
    """
    processor = NativeAppRunProcessor(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    processor.build_bundle()
    processor.process()
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
    force: Optional[bool] = typer.Option(
        False,
        "--force",
        help="Defaults to False. Passing in --force turns this to True, i.e. we will implicitly respond “yes” to any prompts that come up.",
        is_flag=True,
    ),
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
