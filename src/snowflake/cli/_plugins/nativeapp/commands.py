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
from snowflake.cli._plugins.nativeapp.artifacts import VersionInfo
from snowflake.cli._plugins.nativeapp.common_flags import (
    ForceOption,
    InteractiveOption,
    ValidateOption,
)
from snowflake.cli._plugins.nativeapp.entities.application import ApplicationEntityModel
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.nativeapp.release_channel.commands import (
    app as release_channels_app,
)
from snowflake.cli._plugins.nativeapp.release_directive.commands import (
    app as release_directives_app,
)
from snowflake.cli._plugins.nativeapp.sf_facade import get_snowflake_facade
from snowflake.cli._plugins.nativeapp.v2_conversions.compat import (
    find_entity,
    force_project_definition_v2,
)
from snowflake.cli._plugins.nativeapp.version.commands import app as versions_app
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import (
    with_project_definition,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.entities.utils import EntityActions
from snowflake.cli.api.exceptions import (
    IncompatibleParametersError,
    UnmetParametersError,
)
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    ObjectResult,
    StreamResult,
)
from snowflake.cli.api.project.util import same_identifiers
from typing_extensions import Annotated

app = SnowTyperFactory(
    name="app",
    help="Manages a Snowflake Native App",
)
app.add_typer(versions_app)
app.add_typer(release_directives_app)
app.add_typer(release_channels_app)

log = logging.getLogger(__name__)


@app.command("bundle")
@with_project_definition()
@force_project_definition_v2()
def app_bundle(
    **options,
) -> CommandResult:
    """
    Prepares a local folder with configured app artifacts.
    """
    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    package = cli_context.project_definition.entities[package_id]
    ws.perform_action(
        package_id,
        EntityActions.BUNDLE,
    )
    return MessageResult(f"Bundle generated at {ws.project_root / package.deploy_root}")


@app.command("diff", requires_connection=True, hidden=True)
@with_project_definition()
@force_project_definition_v2()
def app_diff(
    **options,
) -> CommandResult | None:
    """
    Performs a diff between the app's source stage and the local deploy root.
    """
    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    diff = ws.perform_action(
        package_id,
        EntityActions.DIFF,
        print_to_console=cli_context.output_format != OutputFormat.JSON,
    )
    if cli_context.output_format == OutputFormat.JSON:
        return ObjectResult(diff.to_dict())

    return None


@app.command("run", requires_connection=True)
@with_project_definition()
@force_project_definition_v2(app_required=True)
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
    channel: str = typer.Option(
        None,
        show_default=False,
        help=f"""The name of the release channel to use when creating or upgrading an application instance from a release directive.
        Requires the `--from-release-directive` flag to be set. If unset, the default channel will be used.""",
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
    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    app_id = options["app_entity_id"]
    app_model = cli_context.project_definition.entities[app_id]
    ws.perform_action(
        app_id,
        EntityActions.DEPLOY,
        validate=validate,
        version=version,
        patch=patch,
        from_release_directive=from_release_directive,
        prune=True,
        recursive=True,
        paths=[],
        interactive=interactive,
        force=force,
        release_channel=channel,
    )
    app = ws.get_entity(app_id)
    return MessageResult(
        f"Your application object ({app_model.fqn.name}) is now available:\n{app.get_snowsight_url()}"
    )


@app.command("open", requires_connection=True)
@with_project_definition()
@force_project_definition_v2(app_required=True)
def app_open(
    **options,
) -> CommandResult:
    """
    Opens the Snowflake Native App inside of your browser,
    once it has been installed in your account.
    """
    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    app_id = options["app_entity_id"]
    app = ws.get_entity(app_id)
    if get_snowflake_facade().get_existing_app_info(app.name, app.role):
        typer.launch(app.get_snowsight_url())
        return MessageResult(f"Snowflake Native App opened in browser.")
    else:
        return MessageResult(
            'Snowflake Native App not yet deployed! Please run "snow app run" first.'
        )


@app.command("teardown", requires_connection=True)
@with_project_definition()
@force_project_definition_v2(single_app_and_package=False)
def app_teardown(
    force: Optional[bool] = ForceOption,
    cascade: Optional[bool] = typer.Option(
        None,
        help=f"""Whether to drop all application objects owned by the application within the account. Default: false.""",
        show_default=False,
    ),
    interactive: bool = InteractiveOption,
    # Same as the param auto-added by @force_project_definition_v2 if single_app_and_package were true
    package_entity_id: Optional[str] = typer.Option(
        default="",
        help="The ID of the package entity on which to operate when the definition_version is 2 or higher.",
    ),
    **options,
) -> CommandResult:
    """
    Attempts to drop both the application object and application package as defined in the project definition file.
    """
    cli_context = get_cli_context()
    project = cli_context.project_definition

    # Determine the package entity to drop, there must be one
    app_package_entity = find_entity(
        project,
        ApplicationPackageEntityModel,
        package_entity_id,
        disambiguation_option="--package-entity-id",
        required=True,
    )
    assert app_package_entity is not None  # satisfy mypy

    # Same implementation as `snow ws drop`
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )

    # TODO: get all apps created from this application package from snowflake, compare, confirm and drop.
    # TODO: add messaging/confirmation here for extra apps found as part of above
    all_packages_with_id = [
        package_entity.entity_id
        for package_entity in project.get_entities_by_type(
            ApplicationPackageEntityModel.get_type()
        ).values()
        if same_identifiers(package_entity.fqn.name, app_package_entity.fqn.name)
    ]

    for app_entity in project.get_entities_by_type(
        ApplicationEntityModel.get_type()
    ).values():
        # Drop each app
        if app_entity.from_.target in all_packages_with_id:
            ws.perform_action(
                app_entity.entity_id,
                EntityActions.DROP,
                force_drop=force,
                interactive=interactive,
                cascade=cascade,
            )
    # Then drop the package
    ws.perform_action(
        app_package_entity.entity_id,
        EntityActions.DROP,
        force_drop=force,
        interactive=interactive,
        cascade=cascade,
    )

    return MessageResult(f"Teardown is now complete.")


@app.command("deploy", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
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
            Paths, relative to the project root, of files or directories you want to upload to a stage. If a file is
            specified, it must match one of the artifacts src pattern entries in snowflake.yml. If a directory is
            specified, it will be searched for subfolders or files to deploy based on artifacts src pattern entries. If
            unspecified, the command syncs all local changes to the stage."""
        ).strip(),
    ),
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    validate: bool = ValidateOption,
    **options,
) -> CommandResult:
    """
    Creates an application package in your Snowflake account and syncs the local changes to the stage without creating or updating the application.
    Running this command with no arguments at all, as in `snow app deploy`, is a shorthand for `snow app deploy --prune --recursive`.
    """
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
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    ws.perform_action(
        package_id,
        EntityActions.DEPLOY,
        prune=prune,
        recursive=recursive,
        paths=paths,
        validate=validate,
        interactive=interactive,
        force=force,
    )

    return MessageResult(
        f"Deployed successfully. Application package and stage are up-to-date."
    )


@app.command("validate", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def app_validate(
    **options,
):
    """
    Validates a deployed Snowflake Native App's setup script.
    """
    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    package = ws.get_entity(package_id)
    if cli_context.output_format == OutputFormat.JSON:
        return ObjectResult(
            package.get_validation_result(
                action_ctx=ws.action_ctx,
                use_scratch_stage=True,
                interactive=False,
                force=True,
            )
        )

    ws.perform_action(
        package_id,
        EntityActions.VALIDATE,
        interactive=False,
        force=True,
    )
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
@force_project_definition_v2(app_required=True)
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
        raise UnmetParametersError(["--consumer-org", "--consumer-account"])

    if follow:
        if until:
            raise IncompatibleParametersError(["--follow", "--until"])
        if first >= 0:
            raise IncompatibleParametersError(["--follow", "--first"])

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    app_id = options["app_entity_id"]

    record_type_names = [r.name for r in record_types]

    if follow and last == -1 and not since:
        # If we don't have a value for --last or --since, assume a value
        # for --last so we at least print something before starting the stream
        last = DEFAULT_EVENT_FOLLOW_LAST
    stream: Iterable[CommandResult] = (
        EventResult(event)
        for event in ws.perform_action(
            app_id,
            EntityActions.EVENTS,
            since=since,
            until=until,
            record_types=record_type_names,
            scopes=scopes,
            consumer_org=consumer_org,
            consumer_account=consumer_account,
            consumer_app_hash=consumer_app_hash,
            first=first,
            last=last,
            follow=follow,
            interval_seconds=follow_interval,
        )
    )
    if follow:
        # Append a newline at the end to make the CLI output clean when we hit Ctrl-C
        stream = itertools.chain(stream, [MessageResult("")])

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


@app.command("publish", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def app_publish(
    version: Optional[str] = typer.Option(
        default=None,
        show_default=False,
        help="The version to publish to the provided release channel and release directive. Version is required to exist unless `--create-version` flag is used.",
    ),
    patch: Optional[int] = typer.Option(
        default=None,
        show_default=False,
        help="The patch number under the given version. This will be used when setting the release directive. Patch is required to exist unless `--create-version` flag is used.",
    ),
    channel: Optional[str] = typer.Option(
        "DEFAULT",
        help="The name of the release channel to publish to. If not provided, the default release channel is used.",
    ),
    directive: Optional[str] = typer.Option(
        "DEFAULT",
        help="The name of the release directive to update with the specified version and patch. If not provided, the default release directive is used.",
    ),
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    create_version: bool = typer.Option(
        False,
        "--create-version",
        help="Create a new version or patch based on the provided `--version` and `--patch` values. Fallback to the manifest values if not provided.",
        is_flag=True,
    ),
    from_stage: bool = typer.Option(
        False,
        "--from-stage",
        help="When enabled, the Snowflake CLI creates a version from the current application package stage without syncing to the stage first. Can only be used with `--create-version` flag.",
        is_flag=True,
    ),
    label: Optional[str] = typer.Option(
        None,
        "--label",
        help="A label for the version that is displayed to consumers. Can only be used with `--create-version` flag.",
    ),
    **options,
) -> CommandResult:
    """
    Adds the version to the release channel and updates the release directive with the new version and patch.
    """
    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    version_info: VersionInfo = ws.perform_action(
        package_id,
        EntityActions.PUBLISH,
        version=version,
        patch=patch,
        release_channel=channel,
        release_directive=directive,
        interactive=interactive,
        force=force,
        create_version=create_version,
        from_stage=from_stage,
        label=label,
    )
    return MessageResult(
        f"Version {version_info.version_name} and patch {version_info.patch_number} published to release directive {directive} of release channel {channel}."
    )
