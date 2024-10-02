# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import itertools
import logging
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import Generator, Iterable, List, Optional, cast

import typer
from snowflake.cli._plugins.nativeapp.common_flags import (
    ForceOption,
    InteractiveOption,
    ValidateOption,
)
from snowflake.cli._plugins.nativeapp.init import (
    OFFICIAL_TEMPLATES_GITHUB_URL,
    nativeapp_init,
)
from snowflake.cli._plugins.nativeapp.manager import NativeAppManager
from snowflake.cli._plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
)
from snowflake.cli._plugins.nativeapp.run_processor import NativeAppRunProcessor
from snowflake.cli._plugins.nativeapp.teardown_processor import (
    NativeAppTeardownProcessor,
)
from snowflake.cli._plugins.nativeapp.utils import (
    get_first_paragraph_from_markdown_file,
    shallow_git_clone,
)
from snowflake.cli._plugins.nativeapp.v2_conversions.v2_to_v1_decorator import (
    nativeapp_definition_v2_to_v1,
)
from snowflake.cli._plugins.nativeapp.version.commands import app as versions_app
from snowflake.cli._plugins.stage.diff import (
    DiffResult,
    compute_stage_diff,
)
from snowflake.cli._plugins.stage.utils import print_diff_to_console
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import (
    with_project_definition,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.exceptions import IncompatibleParametersError
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    ObjectResult,
    StreamResult,
)
from snowflake.cli.api.project.project_verification import assert_project_type
from snowflake.cli.api.secure_path import SecurePath
from typing_extensions import Annotated

app = SnowTyperFactory(
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
        show_default=False,
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
        help="A specific template name within the template repo to use as template for the Snowflake Native App project. Example: Default is basic if `--template-repo` is https://github.com/snowflakedb/native-apps-templates.git, and None if any other --template-repo is specified.",
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
        from git import rmtree as git_rmtree

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

        # proactively clean up here to avoid permission issues on Windows
        repo.close()
        git_rmtree(temp_path.path)

        return CollectionResult(result)


@app.command("bundle")
@with_project_definition()
@nativeapp_definition_v2_to_v1
def app_bundle(
    **options,
) -> CommandResult:
    """
    Prepares a local folder with configured app artifacts.
    """

    assert_project_type("native_app")

    cli_context = get_cli_context()
    manager = NativeAppManager(
        project_definition=cli_context.project_definition.native_app,
        project_root=cli_context.project_root,
    )
    manager.build_bundle()
    return MessageResult(f"Bundle generated at {manager.deploy_root}")


@app.command("diff", requires_connection=True, hidden=True)
@with_project_definition()
@nativeapp_definition_v2_to_v1
def app_diff(
    **options,
) -> CommandResult:
    """
    Performs a diff between the app's source stage and the local deploy root.
    """
    assert_project_type("native_app")

    cli_context = get_cli_context()
    manager = NativeAppManager(
        project_definition=cli_context.project_definition.native_app,
        project_root=cli_context.project_root,
    )
    bundle_map = manager.build_bundle()
    diff: DiffResult = compute_stage_diff(
        local_root=Path(manager.deploy_root), stage_fqn=manager.stage_fqn
    )
    if cli_context.output_format == OutputFormat.JSON:
        return ObjectResult(diff.to_dict())
    else:
        print_diff_to_console(diff, bundle_map)
        return None  # don't print any output


@app.command("run", requires_connection=True)
@with_project_definition()
@nativeapp_definition_v2_to_v1
def app_run(
    version: Optional[str] = typer.Option(
        None,
        help=f"""The version defined in an existing application package from which you want to create an application object.
        The application object and application package names are determined from the project definition file.""",
    ),
    patch: Optional[int] = typer.Option(
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
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    validate: bool = ValidateOption,
    **options,
) -> CommandResult:
    """
    Creates an application package in your Snowflake account, uploads code files to its stage,
    then creates or upgrades an application object from the application package.
    """

    assert_project_type("native_app")

    is_interactive = False
    if force:
        policy = AllowAlwaysPolicy()
    elif interactive:
        is_interactive = True
        policy = AskAlwaysPolicy()
    else:
        policy = DenyAlwaysPolicy()

    cli_context = get_cli_context()
    processor = NativeAppRunProcessor(
        project_definition=cli_context.project_definition.native_app,
        project_root=cli_context.project_root,
    )
    bundle_map = processor.build_bundle()
    processor.process(
        bundle_map=bundle_map,
        policy=policy,
        version=version,
        patch=patch,
        from_release_directive=from_release_directive,
        is_interactive=is_interactive,
        validate=validate,
    )
    return MessageResult(
        f"Your application object ({processor.app_name}) is now available:\n"
        + processor.get_snowsight_url()
    )


@app.command("open", requires_connection=True)
@with_project_definition()
@nativeapp_definition_v2_to_v1
def app_open(
    **options,
) -> CommandResult:
    """
    Opens the Snowflake Native App inside of your browser,
    once it has been installed in your account.
    """

    assert_project_type("native_app")

    cli_context = get_cli_context()
    manager = NativeAppManager(
        project_definition=cli_context.project_definition.native_app,
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
@with_project_definition()
@nativeapp_definition_v2_to_v1
def app_teardown(
    force: Optional[bool] = ForceOption,
    cascade: Optional[bool] = typer.Option(
        None,
        help=f"""Whether to drop all application objects owned by the application within the account. Default: false.""",
        show_default=False,
    ),
    interactive: bool = InteractiveOption,
    **options,
) -> CommandResult:
    """
    Attempts to drop both the application object and application package as defined in the project definition file.
    """

    assert_project_type("native_app")

    cli_context = get_cli_context()
    processor = NativeAppTeardownProcessor(
        project_definition=cli_context.project_definition.native_app,
        project_root=cli_context.project_root,
    )
    processor.process(interactive, force, cascade)
    return MessageResult(f"Teardown is now complete.")


@app.command("deploy", requires_connection=True)
@with_project_definition()
@nativeapp_definition_v2_to_v1
def app_deploy(
    prune: Optional[bool] = typer.Option(
        default=None,
        help=f"""Whether to delete specified files from the stage if they don't exist locally. If set, the command deletes files that exist in the stage, but not in the local filesystem. This option cannot be used when paths are specified.""",
    ),
    recursive: Optional[bool] = typer.Option(
        None,
        "--recursive/--no-recursive",
        "-r",
        help=f"""Whether to traverse and deploy files from subdirectories. If set, the command deploys all files and subdirectories; otherwise, only files in the current directory are deployed.""",
    ),
    paths: Optional[List[Path]] = typer.Argument(
        default=None,
        show_default=False,
        help=dedent(
            f"""
            Paths, relative to the the project root, of files or directories you want to upload to a stage. If a file is
            specified, it must match one of the artifacts src pattern entries in snowflake.yml. If a directory is
            specified, it will be searched for subfolders or files to deploy based on artifacts src pattern entries. If
            unspecified, the command syncs all local changes to the stage."""
        ).strip(),
    ),
    validate: bool = ValidateOption,
    **options,
) -> CommandResult:
    """
    Creates an application package in your Snowflake account and syncs the local changes to the stage without creating or updating the application.
    Running this command with no arguments at all, as in `snow app deploy`, is a shorthand for `snow app deploy --prune --recursive`.
    """

    assert_project_type("native_app")

    has_paths = paths is not None and len(paths) > 0
    if prune is None and recursive is None and not has_paths:
        prune = True
        recursive = True
    else:
        if prune is None:
            prune = False
        if recursive is None:
            recursive = False

    if has_paths and prune:
        raise IncompatibleParametersError(["paths", "--prune"])

    cli_context = get_cli_context()
    manager = NativeAppManager(
        project_definition=cli_context.project_definition.native_app,
        project_root=cli_context.project_root,
    )

    bundle_map = manager.build_bundle()
    manager.deploy(
        bundle_map=bundle_map,
        prune=prune,
        recursive=recursive,
        local_paths_to_sync=paths,
        validate=validate,
    )

    return MessageResult(
        f"Deployed successfully. Application package and stage are up-to-date."
    )


@app.command("validate", requires_connection=True)
@with_project_definition()
@nativeapp_definition_v2_to_v1
def app_validate(**options):
    """
    Validates a deployed Snowflake Native App's setup script.
    """

    assert_project_type("native_app")

    cli_context = get_cli_context()
    manager = NativeAppManager(
        project_definition=cli_context.project_definition.native_app,
        project_root=cli_context.project_root,
    )
    if cli_context.output_format == OutputFormat.JSON:
        return ObjectResult(manager.get_validation_result(use_scratch_stage=True))

    manager.validate(use_scratch_stage=True)
    return MessageResult("Snowflake Native App validation succeeded.")


class RecordType(Enum):
    LOG = "log"
    SPAN = "span"
    SPAN_EVENT = "span_event"


# The default number of lines to print before streaming when running
# snow app events --follow
DEFAULT_EVENT_FOLLOW_LAST = 20


@app.command("events", requires_connection=True)
@with_project_definition()
@nativeapp_definition_v2_to_v1
def app_events(
    since: str = typer.Option(
        default="",
        help="Fetch events that are newer than this time ago, in Snowflake interval syntax.",
    ),
    until: str = typer.Option(
        default="",
        help="Fetch events that are older than this time ago, in Snowflake interval syntax.",
    ),
    record_types: Annotated[
        list[RecordType], typer.Option(case_sensitive=False)
    ] = typer.Option(
        [],
        "--type",
        help="Restrict results to specific record type. Can be specified multiple times.",
    ),
    scopes: Annotated[list[str], typer.Option()] = typer.Option(
        [],
        "--scope",
        help="Restrict results to a specific scope name. Can be specified multiple times.",
    ),
    consumer_org: str = typer.Option(
        default="", help="The name of the consumer organization."
    ),
    consumer_account: str = typer.Option(
        default="",
        help="The name of the consumer account in the organization.",
    ),
    consumer_app_hash: str = typer.Option(
        default="",
        help="The SHA-1 hash of the consumer application name",
    ),
    first: int = typer.Option(
        default=-1,
        show_default=False,
        help="Fetch only the first N events. Cannot be used with --last.",
    ),
    last: int = typer.Option(
        default=-1,
        show_default=False,
        help="Fetch only the last N events. Cannot be used with --first.",
    ),
    follow: bool = typer.Option(
        False,
        "--follow",
        "-f",
        help=(
            f"Continue polling for events. Implies --last {DEFAULT_EVENT_FOLLOW_LAST} "
            f"unless overridden or the --since flag is used."
        ),
    ),
    follow_interval: int = typer.Option(
        10,
        help=f"Polling interval in seconds when using the --follow flag.",
    ),
    **options,
):
    """Fetches events for this app from the event table configured in Snowflake.

    By default, this command will fetch events generated by an app installed in the
    current connection's account. To fetch events generated by an app installed
    in a consumer account, use the --consumer-org and --consumer-account options.
    This requires event sharing to be set up to route events to the provider account:
    https://docs.snowflake.com/en/developer-guide/native-apps/setting-up-logging-and-events
    """
    if first >= 0 and last >= 0:
        raise IncompatibleParametersError(["--first", "--last"])

    if (consumer_org and not consumer_account) or (
        consumer_account and not consumer_org
    ):
        raise IncompatibleParametersError(["--consumer-org", "--consumer-account"])

    if follow:
        if until:
            raise IncompatibleParametersError(["--follow", "--until"])
        if first >= 0:
            raise IncompatibleParametersError(["--follow", "--first"])

    assert_project_type("native_app")

    record_type_names = [r.name for r in record_types]
    manager = NativeAppManager(
        project_definition=get_cli_context().project_definition.native_app,
        project_root=get_cli_context().project_root,
    )
    if follow:
        if last == -1 and not since:
            # If we don't have a value for --last or --since, assume a value
            # for --last so we at least print something before starting the stream
            last = DEFAULT_EVENT_FOLLOW_LAST
        stream: Iterable[CommandResult] = (
            EventResult(event)
            for event in manager.stream_events(
                since=since,
                last=last,
                interval_seconds=follow_interval,
                record_types=record_type_names,
                scopes=scopes,
                consumer_org=consumer_org,
                consumer_account=consumer_account,
                consumer_app_hash=consumer_app_hash,
            )
        )
        # Append a newline at the end to make the CLI output clean when we hit Ctrl-C
        stream = itertools.chain(stream, [MessageResult("")])
    else:
        stream = (
            EventResult(event)
            for event in manager.get_events(
                since=since,
                until=until,
                record_types=record_type_names,
                scopes=scopes,
                first=first,
                last=last,
                consumer_org=consumer_org,
                consumer_account=consumer_account,
                consumer_app_hash=consumer_app_hash,
            )
        )

    # Cast the stream to a Generator since that's what StreamResult wants
    return StreamResult(cast(Generator[CommandResult, None, None], stream))


class EventResult(ObjectResult, MessageResult):
    """ObjectResult that renders as a custom string when not printed as JSON."""

    @property
    def message(self):
        e = self._element
        return f"{e['TIMESTAMP']} {e['VALUE']}"

    @property
    def result(self):
        return self._element
