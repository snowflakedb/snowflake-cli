import logging
from pathlib import Path
from typing import List, Optional

import typer
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.decorators import (
    with_project_definition,
)
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.nativeapp.common_flags import ForceOption, InteractiveOption
from snowflake.cli.plugins.nativeapp.init import (
    OFFICIAL_TEMPLATES_GITHUB_URL,
    nativeapp_init,
)
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
from snowflake.cli.plugins.nativeapp.utils import (
    get_first_paragraph_from_markdown_file,
    is_tty_interactive,
    shallow_git_clone,
)
from snowflake.cli.plugins.nativeapp.version.commands import app as versions_app

app = SnowTyper(
    name="app",
    help="Manages a Snowflake Native App",
)
app.add_typer(versions_app)

log = logging.getLogger(__name__)


@app.command("init")
def app_init(
    path: str = typer.Argument(
        ...,
        help=f"""Directory to be initialized with the Snowflake Native App project. This directory must not already exist.""",
    ),
    name: str = typer.Option(
        None,
        help=f"""The name of the Snowflake Native App project to include in snowflake.yml. When not specified, it is
        generated from the name of the directory. Names are assumed to be unquoted identifiers whenever possible, but
        can be forced to be quoted by including the surrounding quote characters in the provided value.""",
    ),
    template_repo: str = typer.Option(
        None,
        help=f"""Specifies the git URL to a template repository, which can be a template itself or contain many templates inside it,
        such as https://github.com/snowflakedb/native-apps-templates.git for all official Snowflake Native App with Snowflake CLI templates.
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
    Initializes a Snowflake Native App project.
    """
    project = nativeapp_init(
        path=path, name=name, git_url=template_repo, template=template
    )
    return MessageResult(
        f"Snowflake Native App project {project.name} has been created at: {path}"
    )


@app.command("list-templates", hidden=True)
def app_list_templates(**options) -> CommandResult:
    """
    Prints information regarding the official templates that can be used with snow app init.
    """
    with SecurePath.temporary_directory() as temp_path:
        repo = shallow_git_clone(OFFICIAL_TEMPLATES_GITHUB_URL, temp_path.path)

        # Mark a directory as a template if a project definition jinja template is inside
        template_directories = [
            entry.name
            for entry in repo.head.commit.tree
            if (temp_path / entry.name / "snowflake.yml.jinja").exists()
        ]

        # get the template descriptions from the README.md in its directory
        template_descriptions = [
            get_first_paragraph_from_markdown_file(
                (temp_path / directory / "README.md").path
            )
            for directory in template_directories
        ]

        result = (
            {"template": directory, "description": description}
            for directory, description in zip(
                template_directories, template_descriptions
            )
        )

        return CollectionResult(result)


@app.command("bundle", hidden=True)
@with_project_definition("native_app")
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


@app.command("run", requires_connection=True)
@with_project_definition("native_app")
def app_run(
    version: Optional[str] = typer.Option(
        None,
        help=f"""The version defined in an existing application package from which you want to create an application object.
        The application object and application package names are determined from the project definition file.""",
    ),
    patch: Optional[str] = typer.Option(
        None,
        "--patch",
        help=f"""The patch number under the given `--version` defined in an existing application package that should be used to create an application object.
        The application object and application package names are determined from the project definition file.""",
    ),
    from_release_directive: Optional[bool] = typer.Option(
        False,
        "--from-release-directive",
        help=f"""Creates or upgrades an application object to the version and patch specified by the release directive applicable to your Snowflake account.
        The command fails if no release directive exists for your Snowflake account for a given application package, which is determined from the project definition file. Default: unset.""",
        is_flag=True,
    ),
    interactive: Optional[bool] = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """
    Creates an application package in your Snowflake account, uploads code files to its stage,
    then creates or upgrades an application object from the application package.
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
        f"Your application object ({processor.app_name}) is now available:\n"
        + processor.get_snowsight_url()
    )


@app.command("open", requires_connection=True)
@with_project_definition("native_app")
def app_open(
    **options,
) -> CommandResult:
    """
    Opens the Snowflake Native App inside of your browser,
    once it has been installed in your account.
    """
    manager = NativeAppManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    if manager.get_existing_app_info():
        typer.launch(manager.get_snowsight_url())
        return MessageResult(f"Snowflake Native App opened in browser.")
    else:
        return MessageResult(
            'Snowflake Native App not yet deployed! Please run "snow app run" first.'
        )


@app.command("teardown", requires_connection=True)
@with_project_definition("native_app")
def app_teardown(
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """
    Attempts to drop both the application object and application package as defined in the project definition file.
    """
    processor = NativeAppTeardownProcessor(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    processor.process(force)
    return MessageResult(f"Teardown is now complete.")


@app.command("deploy", requires_connection=True)
@with_project_definition("native_app")
def app_deploy(
    prune: Optional[bool] = typer.Option(
        default=None,
        help=f"""Whether to delete specified files from the stage if they don't exist locally. If set, the command deletes files that exist in the stage, but not in the local filesystem.""",
    ),
    recursive: Optional[bool] = typer.Option(
        None,
        "--recursive",
        "-r",
        help=f"""Whether to traverse and deploy files from subdirectories. If set, the command deploys all files and subdirectories; otherwise, only files in the current directory are deployed.""",
    ),
    files: Optional[List[Path]] = typer.Argument(
        default=None,
        show_default=False,
        help=f"""Paths, relative to the the project root, of files you want to upload to a stage. The paths must match one of the artifacts src pattern entries in snowflake.yml. If unspecified, the command syncs all local changes to the stage.""",
    ),
    **options,
) -> CommandResult:
    """
    Creates an application package in your Snowflake account and syncs the local changes to the stage without creating or updating the application.
    Running this command with no arguments at all, as in `snow app deploy`, is a shorthand for `snow app deploy --prune --recursive`.
    """
    has_files = files is not None and len(files) > 0
    if prune is None and recursive is None and not has_files:
        prune = True
        recursive = True
    else:
        if prune is None:
            prune = False
        if recursive is None:
            recursive = False

    manager = NativeAppManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )

    mapped_files = manager.build_bundle()
    manager.deploy(prune, recursive, files, mapped_files)

    return MessageResult(
        f"Deployed successfully. Application package and stage are up-to-date."
    )
