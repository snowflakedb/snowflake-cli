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
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    bullet,
    bullet_list,
    code,
    link,
    plain_text,
)
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

_STREAMLIT_RELATED = (
    link("/developer-guide/snowflake-cli/index"),
    link(
        "/developer-guide/snowflake-cli/command-reference/overview",
        "Snowflake CLI command reference",
    ),
    link(
        "/developer-guide/snowflake-cli/command-reference/streamlit-commands/overview",
        "Streamlit commands",
    ),
)

_STREAMLIT_OBJECT_ALIAS_DOCS = CommandDocs(
    related=_STREAMLIT_RELATED,
    usage_notes=(plain_text("None."),),
)

add_object_command_aliases(
    app=app,
    object_type=ObjectType.STREAMLIT,
    name_argument=StreamlitNameArgument,
    like_option=like_option(
        help_example='`list --like "my%"` lists all streamlit apps that begin with “my”'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
    list_docs=_STREAMLIT_OBJECT_ALIAS_DOCS,
    describe_docs=_STREAMLIT_OBJECT_ALIAS_DOCS,
    drop_docs=_STREAMLIT_OBJECT_ALIAS_DOCS,
)

_EXECUTE_DOCS = CommandDocs(
    related=_STREAMLIT_RELATED,
    usage_notes=(
        plain_text(
            "The command allows a Streamlit app to be executed without user "
            "interaction, such as for batch processing or automation tasks."
        ),
        plain_text(
            "Before executing this command, the following requirements must be met:"
        ),
        bullet_list(
            bullet("You must have a valid Snowflake connection."),
            bullet("The app must already be deployed in the Snowflake environment."),
            bullet(
                "A valid configuration ",
                code("snowflake.yml"),
                " file must exist with the ",
                code("query_warehouse"),
                " and ",
                code("stage"),
                " settings defined.",
            ),
        ),
        plain_text(
            "The application logic, such as calculations and file processing, runs "
            "as if the app were displayed, but does not render any user-visible "
            "output."
        ),
        plain_text(
            "You must ensure that your Snowflake account, database, schema, and "
            "warehouse are properly configured before running the command."
        ),
        plain_text(
            "If an error, such as an invalid database configuration or missing "
            "files, occurs during execution, the command displays an error message "
            "in the terminal."
        ),
    ),
    examples=(
        Example(
            description=plain_text(
                "Execute the ",
                code("my_streamlit_app"),
                " app in the current process without displaying any output.",
            ),
            command="snow streamlit execute my_streamlit_app",
        ),
        Example(
            description=plain_text(
                "Retrieve the URL for the application after execution and open it "
                "in your default web browser."
            ),
            command="snow streamlit get-url my_streamlit_app --open",
        ),
    ),
)


@app.command(requires_connection=True, docs=_EXECUTE_DOCS)
def execute(
    name: FQN = StreamlitNameArgument,
    **options,
):
    """
    Executes a streamlit in a headless mode.
    """
    _ = StreamlitManager().execute(app_name=name)
    return MessageResult(f"Streamlit {name} executed.")


_SHARE_DOCS = CommandDocs(
    related=_STREAMLIT_RELATED,
    usage_notes=(plain_text("None."),),
    examples=(
        Example(
            description=plain_text(
                "The following example shares ",
                code("my-app"),
                " with the custom ",
                code("analyst"),
                " role:",
            ),
            command="snow streamlit share my-app analyst",
        ),
    ),
)


@app.command("share", requires_connection=True, docs=_SHARE_DOCS)
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


_DEPLOY_DOCS = CommandDocs(
    related=_STREAMLIT_RELATED,
    usage_notes=(
        plain_text(
            "This command creates a Streamlit app object in the database and a schema "
            "configured in the specified ",
            code("connection"),
            ".",
        ),
        plain_text(
            "The command uploads local files to a specified stage and creates a Streamlit "
            "app using those files. You must specify the main Python file and query "
            "warehouse. By default, the command uploads the ",
            code("environment.yml"),
            " and ",
            code("pages/"),
            " folder if present. The Streamlit app is created in the database and "
            "schema configured in the specified ",
            code("connection"),
            ".",
        ),
        plain_text(
            "If you don't specify a stage name, the ",
            code("streamlit"),
            " stage is used. If the specified stage does not exist, the command "
            "creates it. You can modify the behavior by using ",
            link(
                "/developer-guide/snowflake-cli/command-reference/streamlit-commands/deploy",
                "command-line options",
            ),
            ".",
        ),
        plain_text(
            "If you specify the ",
            code("--replace"),
            " option, the command uploads new files and overwrites existing files. It "
            "does not remove any files already on the stage.",
        ),
        plain_text(
            "If you specify the ",
            code("--prune"),
            " option, the command removes files that exist in the stage, but not files "
            "in the local filesystem.",
        ),
    ),
    examples=(
        Example(
            command="snow streamlit deploy demo_app --replace",
            output=(
                "Streamlit successfully deployed and available under "
                "https://app.snowflake.com/myorg/myacc/#/streamlit-apps/JDOE.PUBLIC.DEMO_APP"
            ),
        ),
    ),
)


@app.command("deploy", requires_connection=True, docs=_DEPLOY_DOCS)
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
    Deploys a Streamlit app defined in the project definition file (snowflake.yml).
    The bundle always includes main_file. If environment.yml or a pages directory
    (pages_dir, or pages/ by default) exists in the project root, they are included
    even when omitted from artifacts. The stage name comes from the entity's stage
    field (default: streamlit); the command creates that stage if needed. If the
    app already exists, re-run with --replace to update it. If multiple Streamlits
    are defined and no entity_id is provided, the command raises an error.
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


_GET_URL_DOCS = CommandDocs(
    related=_STREAMLIT_RELATED,
    usage_notes=(
        plain_text(
            "The ",
            code("streamlit get-url"),
            " command returns a url link to an existing Streamlit application. You can "
            "also use the ",
            code("--open"),
            " option to automatically open the Streamlit in a new tab in your browser.",
        ),
        plain_text("Note the following requirements:"),
        bullet_list(
            bullet("The app must already be deployed."),
            bullet("You must use the same connection that was used to deploy the app."),
            bullet(
                "If your app is running under different database and schema than "
                "specified in the connection, you must provide them in name as a "
                "fully-qualified name, such as ",
                code("database.schema.name"),
                ".",
            ),
        ),
    ),
    examples=(
        Example(
            description=plain_text(
                "Get a URL for an app using the database and schema specified in the "
                "default connection and opens it in your browser:"
            ),
            command="snow streamlit get-url my_streamlit_app --open",
            output=(
                "https://snowflake.com/provider-deduced-from-connection/#/streamlit-apps/"
                "DB.PUBLIC.MY_STREAMLIT_APP"
            ),
        ),
        Example(
            description=plain_text(
                "Get a URL for an app using a fully-qualified database and schema name:"
            ),
            command="snow streamlit get-url database.schema.my_streamlit_app",
            output=(
                "https://snowflake.com/provider-deduced-from-connection/#/streamlit-apps/"
                "DATABASE.SCHEMA.MY_STREAMLIT_APP"
            ),
        ),
    ),
)


@app.command("get-url", requires_connection=True, docs=_GET_URL_DOCS)
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


_LOGS_DOCS = CommandDocs(
    related=_STREAMLIT_RELATED,
    usage_notes=(
        plain_text(
            "The ",
            code("streamlit logs"),
            " command attaches to the running container of a deployed Streamlit app and "
            "streams log entries until you stop the command with Ctrl+C. Note the "
            "following requirements:",
        ),
        bullet_list(
            bullet(
                "The Streamlit app must already be deployed and running on the SPCSv2 "
                "container runtime. If the app uses an earlier runtime, the command exits "
                "with an error from the runtime check."
            ),
            bullet(
                "You must use the same Snowflake connection (or override values) that has "
                "access to the app's database, schema, and role."
            ),
            bullet(
                "Pair the command with the global ",
                code("--format"),
                " option to convert the live stream to JSON or CSV for downstream piping. "
                "For example, ",
                code("snow streamlit logs my_app --format json | jq ..."),
                ".",
            ),
            bullet(
                "Use the ",
                code("--tail"),
                " option to control how many historical log lines are sent before the live "
                "stream begins. Pass ",
                code("--tail 0"),
                " to receive only new log entries.",
            ),
        ),
    ),
    examples=(
        Example(
            description=plain_text(
                "Stream live logs (and the most recent 100 historical lines) for the "
                "Streamlit app defined in the current project's ",
                code("snowflake.yml"),
                ":",
            ),
            command="snow streamlit logs",
        ),
        Example(
            description=plain_text(
                "Stream live logs for a specific app by fully qualified name, without a "
                "project definition:"
            ),
            command="snow streamlit logs --name my_db.public.my_streamlit_app",
        ),
        Example(
            description=plain_text(
                "Stream only live entries (no historical lines) and pipe the "
                "JSON-formatted stream to another tool:"
            ),
            command="snow streamlit logs my_streamlit --tail 0 --format json",
        ),
        Example(
            description=plain_text(
                "When several Streamlit entities are defined in ",
                code("snowflake.yml"),
                ", target one by entity ID:",
            ),
            command="snow streamlit logs my_streamlit_entity --tail 500",
        ),
    ),
)


@app.command("logs", requires_connection=True, docs=_LOGS_DOCS)
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
