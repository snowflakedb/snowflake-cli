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

import logging
from pathlib import Path
from typing import List, NamedTuple, Optional

import click
import typer
from click import ClickException
from snowflake.cli._plugins.object.command_aliases import (
    add_object_command_aliases,
    scope_option,
)
from snowflake.cli._plugins.streamlit.log_streaming import (
    stream_logs,
    validate_spcs_v2_runtime,
)
from snowflake.cli._plugins.streamlit.manager import StreamlitManager
from snowflake.cli._plugins.streamlit.project_grants import add_grants
from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import (
    with_experimental_behaviour,
    with_project_definition,
)
from snowflake.cli.api.commands.flags import (
    IdentifierType,
    PruneOption,
    ReplaceOption,
    entity_argument,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.commands.utils import get_entity_for_operation
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.console.console import CliConsole
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.entities.utils import EntityActions
from snowflake.cli.api.exceptions import CliArgumentError, NoProjectDefinitionError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    MultipleResults,
    SingleQueryResult,
    StreamResult,
)
from snowflake.cli.api.project.definition_conversion import (
    convert_project_definition_to_v2,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.schemas.entities.common import Grant
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath

app = SnowTyperFactory(
    name="streamlit",
    help="Manages a Streamlit app in Snowflake.",
)
log = logging.getLogger(__name__)

StreamlitNameArgument = identifier_argument(
    sf_object="Streamlit app", example="my_streamlit"
)
OpenOption = typer.Option(
    False,
    "--open",
    help="Whether to open the Streamlit app in a browser.",
    is_flag=True,
)


add_object_command_aliases(
    app=app,
    object_type=ObjectType.STREAMLIT,
    name_argument=StreamlitNameArgument,
    like_option=like_option(
        help_example='`list --like "my%"` lists all streamlit apps that begin with “my”'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
)


@app.command(requires_connection=True)
def execute(
    name: FQN = StreamlitNameArgument,
    **options,
):
    """
    Executes a streamlit in a headless mode.
    """
    _ = StreamlitManager().execute(app_name=name)
    return MessageResult(f"Streamlit {name} executed.")


@app.command("share", requires_connection=True)
@with_project_definition(is_optional=True)
def streamlit_share(
    name: FQN = StreamlitNameArgument,
    to_role: Optional[str] = typer.Argument(
        None,
        help="Role with which to share the Streamlit app.",
        show_default=False,
    ),
    to_user: Optional[List[str]] = typer.Option(
        None,
        "--to-user",
        help="User with which to share the Streamlit app, instead of a role."
        " Repeat the option to share with several users.",
        show_default=False,
        hidden=not FeatureFlag.ENABLE_STREAMLIT_UBAC_SHARING.is_enabled(),
    ),
    with_grant_option: bool = typer.Option(
        False,
        "--with-grant-option",
        help="Also let the grantee share the app with others.",
        is_flag=True,
    ),
    grant_location_usage: bool = typer.Option(
        False,
        "--grant-location-usage",
        help="Also grant the grantee USAGE on the database and schema holding the app.",
        is_flag=True,
    ),
    **options,
) -> CommandResult:
    """
    Shares a Streamlit app with another role.
    """
    # Role stays positional, so existing invocations keep working.
    if to_role and to_user:
        raise CliArgumentError("Share with a role or with --to-user, not both.")
    if not to_role and not to_user:
        raise CliArgumentError("Name the role to share with, or a user via --to-user.")

    # Every share this command issues is USAGE, so the grantees are Grants.
    def _usage(**grantee) -> Grant:
        return Grant(privilege="USAGE", with_grant_option=with_grant_option, **grantee)

    grantees = (
        [_usage(role=to_role)]
        if to_role
        else [_usage(user=user) for user in to_user or []]
    )
    manager = StreamlitManager()
    # A copy: `using_connection` fills in the connection's database and schema in
    # place, and the GRANT below deliberately uses the name as given.
    resolved = FQN(database=name.database, schema=name.schema, name=name.name)
    resolved.using_connection(get_cli_context().connection)
    # Located before the GRANT: reading the project definition validates the whole
    # of snowflake.yml, and a problem in an entity unrelated to this app must not
    # surface as a failure once the share has already gone through.
    users = [grantee for grantee in grantees if grantee.user]
    target = _grants_target(resolved) if users else _GrantsTarget()

    cursors = manager.share(
        streamlit_name=name,
        grantees=grantees,
        with_grant_option=with_grant_option,
    )
    _handle_location_usage(manager, resolved, grantees, grant_location_usage)
    _record_user_grants(target, users)
    # One grantee keeps the single-result shape this command has always returned.
    if len(cursors) == 1:
        return SingleQueryResult(cursors[0])
    return MultipleResults(SingleQueryResult(cursor) for cursor in cursors)


class _GrantsTarget(NamedTuple):
    """The `grants:` a user share is to be recorded under, or why it cannot be.

    Empty throughout means there is nothing to record against — no project file,
    or no entity in one that names this app — and nothing to report either.
    """

    project_file: Optional[SecurePath] = None
    entity_id: Optional[str] = None
    reason: Optional[str] = None


def _grants_target(name: FQN) -> _GrantsTarget:
    """Find the entity whose `grants:` should record a share of this app.

    Called before the share is issued, because the first read of
    `project_definition` pydantic-validates the entire project file: a schema
    error anywhere in it would otherwise be raised after the GRANT had already
    been made, reporting a share that succeeded as a failure. Recording is a
    convenience, so a project file that cannot be read is carried back as a
    reason to warn about rather than raised.
    """
    ctx = get_cli_context()
    try:
        entity_id = _streamlit_entity_id(ctx.project_definition, name, ctx.connection)
        if entity_id is None:
            log.debug("No streamlit entity matches %s; snowflake.yml left alone", name)
            return _GrantsTarget()
        project_root = ctx.project_root
    except Exception as error:
        log.debug("Could not read the project definition", exc_info=True)
        return _GrantsTarget(
            reason=f"{DefinitionManager.BASE_DEFINITION_FILENAME} could not be"
            f" read: {error}"
        )
    return _GrantsTarget(
        SecurePath(project_root) / DefinitionManager.BASE_DEFINITION_FILENAME,
        entity_id,
    )


def _record_user_grants(target: _GrantsTarget, users: list[Grant]) -> None:
    """Add the user shares to `grants:` in snowflake.yml, so a redeploy keeps them.

    Only user shares: a role share predates this command and writing those would
    churn project files that never asked for it. Nothing happens outside a
    project, or when no entity in it names this app.
    """
    if not users:
        return
    if target.reason:
        reason: Optional[str] = target.reason
    elif target.project_file and target.entity_id:
        reason = add_grants(target.project_file, target.entity_id, users)
    else:
        return

    if reason:
        entries = "\n".join(
            f"      - privilege: USAGE\n"
            f"        user: {sanitize_for_terminal(grant.user or '')}"
            for grant in users
        )
        cc.warning(
            f"Could not record the share in snowflake.yml, because"
            f" {sanitize_for_terminal(reason)}. Add it under the app's grants to keep"
            f" the share on the next deploy:\n{entries}"
        )
    else:
        cc.step(f"Recorded the share in {DefinitionManager.BASE_DEFINITION_FILENAME}.")


def _streamlit_entity_id(project_definition, name: FQN, conn) -> Optional[str]:
    """The id of the streamlit entity that names this app, if the project has one.

    Both sides are resolved against the connection first, so a project file that
    leaves the database and schema implicit still matches a qualified argument.
    """
    entities = getattr(project_definition, "entities", None) or {}
    for entity_id, entity in entities.items():
        if entity.get_type() != ObjectType.STREAMLIT.value.cli_name:
            continue
        # `entity.fqn` builds a new FQN per access, so resolving it here does not
        # write the connection's defaults back into the project definition.
        if entity.fqn.using_connection(conn).identifier.upper() == (
            name.identifier.upper()
        ):
            return entity_id
    return None


def _handle_location_usage(
    manager: StreamlitManager,
    name: FQN,
    grantees: list[Grant],
    grant_location_usage: bool,
) -> None:
    """Grant USAGE on the app's database and schema, or say why it was not.

    Opt-in rather than automatic: the database and schema hold objects beyond
    this app, so widening access to them is a larger grant than the one asked
    for, and the app grant itself does not need it.
    """
    if grant_location_usage:
        for problem in manager.grant_location_usage(name, grantees):
            cc.warning(problem)
        return
    # Only for users: a role usually reaches the app's schema already, and
    # warning every time would be noise.
    if any(grantee.user for grantee in grantees) and name.database:
        cc.warning(
            f"A user may also need USAGE on {sanitize_for_terminal(name.prefix)}"
            " to open the app. Re-run with --grant-location-usage to grant it."
        )


def _default_file_callback(param_name: str):
    from click.core import ParameterSource  # type: ignore

    def _check_file_exists_if_not_default(ctx: click.Context, value):
        if (
            ctx.get_parameter_source(param_name) != ParameterSource.DEFAULT  # type: ignore
            and value
            and not Path(value).exists()
        ):
            raise ClickException(f"Provided file {value} does not exist")
        return Path(value)

    return _check_file_exists_if_not_default


LegacyOption = typer.Option(
    False,
    "--legacy",
    help="Use legacy ROOT_LOCATION SQL syntax.",
    is_flag=True,
)


@app.command("deploy", requires_connection=True)
@with_project_definition()
@with_experimental_behaviour()  # Kept for backward compatibility
def streamlit_deploy(
    replace: bool = ReplaceOption(
        help="Replaces the Streamlit app if it already exists. It only uploads new and overwrites existing files, "
        "but does not remove any files already on the stage."
    ),
    prune: bool = PruneOption(),
    entity_id: str = entity_argument("streamlit"),
    open_: bool = OpenOption,
    legacy: bool = LegacyOption,
    **options,
) -> CommandResult:
    """
    Deploys a Streamlit app defined in the project definition file (snowflake.yml). By default, the command uploads
    environment.yml and any other pages or folders, if present. If you don't specify a stage name, the `streamlit`
    stage is used. If the specified stage does not exist, the command creates it. If multiple Streamlits are defined
    in snowflake.yml and no entity_id is provided then command will raise an error.
    """

    cli_context = get_cli_context()
    workspace_ctx = _get_current_workspace_context()

    # Handle deprecated --experimental flag for backward compatibility
    if options.get("experimental"):
        workspace_ctx.console.warning(
            "[Deprecation] The --experimental flag is deprecated. "
            "Versioned deployment is now the default behavior. "
            "This flag will be removed in a future version."
        )

    pd = cli_context.project_definition
    if not pd.meets_version_requirement("2"):
        if not pd.streamlit:
            raise NoProjectDefinitionError(
                project_type="streamlit", project_root=cli_context.project_root
            )
        pd = convert_project_definition_to_v2(cli_context.project_root, pd)

    streamlit: StreamlitEntity = StreamlitEntity(
        entity_model=get_entity_for_operation(
            cli_context=cli_context,
            entity_id=entity_id,
            project_definition=pd,
            entity_type=ObjectType.STREAMLIT.value.cli_name,
        ),
        workspace_ctx=workspace_ctx,
    )

    url = streamlit.perform(
        EntityActions.DEPLOY,
        ActionContext(
            get_entity=lambda *args: None,
        ),
        _open=open_,
        replace=replace,
        legacy=legacy,
        prune=prune,
    )

    if open_:
        typer.launch(url)

    return MessageResult(f"Streamlit successfully deployed and available under {url}")


@app.command("get-url", requires_connection=True)
def get_url(
    name: FQN = StreamlitNameArgument,
    open_: bool = OpenOption,
    **options,
):
    """Returns a URL to the specified Streamlit app"""
    url = StreamlitManager().get_url(streamlit_name=name)
    if open_:
        typer.launch(url)
    return MessageResult(url)


@app.command("logs", requires_connection=True)
@with_project_definition(is_optional=True)
def streamlit_logs(
    entity_id: str = entity_argument("streamlit"),
    name: FQN = typer.Option(
        None,
        "--name",
        help="Fully qualified name of the Streamlit app (e.g. my_app, schema.my_app, or db.schema.my_app). "
        "Overrides the project definition when provided.",
        click_type=IdentifierType(),
    ),
    tail: int = typer.Option(
        100,
        "--tail",
        "-n",
        min=0,
        max=1000,  # server-side buffer size limit (see logs_service.proto)
        help="Number of historical log lines to fetch. Use 0 for live logs only.",
    ),
    **options,
) -> CommandResult:
    """
    Streams live logs from a deployed Streamlit app to your terminal.

    Reads the Streamlit app name from the project definition file (snowflake.yml)
    or from the --name option. Connects to the app's developer log service via
    WebSocket and prints log entries in real time. Press Ctrl+C to stop streaming.

    Log streaming requires SPCSv2 runtime.
    """
    cli_context = get_cli_context()
    conn = cli_context.connection

    if name is not None:
        if entity_id is not None:
            raise CliArgumentError(
                "Cannot specify both --name and an entity ID. "
                "Use --name to identify the app directly, or use an "
                "entity ID to reference a snowflake.yml definition."
            )
        log.debug("Resolving Streamlit FQN from --name flag: %s", name)
        fqn = name.using_connection(conn)
    else:
        log.debug("No --name provided; resolving Streamlit from project definition")
        pd = cli_context.project_definition
        if pd is None:
            raise CliArgumentError(
                "No Streamlit app specified. Provide --name or run from a "
                "directory with a snowflake.yml project definition."
            )
        if not pd.meets_version_requirement("2"):
            if not pd.streamlit:
                raise NoProjectDefinitionError(
                    project_type="streamlit", project_root=cli_context.project_root
                )
            pd = convert_project_definition_to_v2(cli_context.project_root, pd)

        entity_model = get_entity_for_operation(
            cli_context=cli_context,
            entity_id=entity_id,
            project_definition=pd,
            entity_type=ObjectType.STREAMLIT.value.cli_name,
        )

        fqn = entity_model.fqn.using_connection(conn)

    log.debug("Validating SPCSv2 runtime for %s via DESCRIBE STREAMLIT", fqn)
    validate_spcs_v2_runtime(conn, fqn)

    return StreamResult(stream_logs(conn=conn, fqn=str(fqn), tail_lines=tail))


def _get_current_workspace_context():
    ctx = get_cli_context()

    return WorkspaceContext(
        console=CliConsole(),
        project_root=ctx.project_root,
        get_default_role=lambda: ctx.connection.role,
        get_default_warehouse=lambda: ctx.connection.warehouse,
    )
