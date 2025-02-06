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
from typing import Optional

import typer
from snowflake.cli._plugins.nativeapp.constants import DEFAULT_CHANNEL
from snowflake.cli._plugins.nativeapp.v2_conversions.compat import (
    force_project_definition_v2,
)
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import like_option
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.entities.utils import EntityActions
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
)

app = SnowTyperFactory(
    name="release-directive",
    help="Manages release directives of an application package",
)

log = logging.getLogger(__name__)


@app.command("list", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def release_directive_list(
    like: str = like_option(
        help_example="`snow app release-directive list --like='my%'` lists all release directives starting with 'my'",
    ),
    channel: Optional[str] = typer.Option(
        default=None,
        show_default=False,
        help="The release channel to use when listing release directives. If not provided, release directives from all release channels are listed.",
    ),
    **options,
) -> CommandResult:
    """
    Lists release directives in an application package.
    If no release channel is specified, release directives for all channels are listed.
    If a release channel is specified, only release directives for that channel are listed.

    If `--like` is provided, only release directives matching the SQL pattern are listed.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    result = ws.perform_action(
        package_id,
        EntityActions.RELEASE_DIRECTIVE_LIST,
        release_channel=channel,
        like=like,
    )

    return CollectionResult(result)


@app.command("set", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def release_directive_set(
    directive: str = typer.Argument(
        show_default=False,
        help="Name of the release directive to set",
    ),
    channel: str = typer.Option(
        DEFAULT_CHANNEL,
        help="Name of the release channel to use",
    ),
    target_accounts: Optional[str] = typer.Option(
        None,
        show_default=False,
        help="List of the accounts to apply the release directive to. Format must be `org1.account1,org2.account2`",
    ),
    version: str = typer.Option(
        show_default=False,
        help="Version of the application package to use",
    ),
    patch: int = typer.Option(
        show_default=False,
        help="Patch number to use for the selected version",
    ),
    **options,
) -> CommandResult:
    """
    Sets a release directive.

    target_accounts cannot be specified for default release directives.
    target_accounts field is required when creating a new non-default release directive.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    ws.perform_action(
        package_id,
        EntityActions.RELEASE_DIRECTIVE_SET,
        release_directive=directive,
        version=version,
        patch=patch,
        target_accounts=None if target_accounts is None else target_accounts.split(","),
        release_channel=channel,
    )
    return MessageResult("Successfully set release directive.")


@app.command("unset", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def release_directive_unset(
    directive: str = typer.Argument(
        show_default=False,
        help="Name of the release directive",
    ),
    channel: str = typer.Option(
        DEFAULT_CHANNEL,
        help="Name of the release channel to use",
    ),
    **options,
) -> CommandResult:
    """
    Unsets a release directive.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    ws.perform_action(
        package_id,
        EntityActions.RELEASE_DIRECTIVE_UNSET,
        release_directive=directive,
        release_channel=channel,
    )
    return MessageResult(f"Successfully unset release directive {directive}.")


@app.command("add-accounts", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def release_directive_add_accounts(
    directive: str = typer.Argument(
        show_default=False,
        help="Name of the release directive",
    ),
    channel: str = typer.Option(
        DEFAULT_CHANNEL,
        help="Name of the release channel to use",
    ),
    target_accounts: str = typer.Option(
        show_default=False,
        help="List of the accounts to add to the release directive. Format must be `org1.account1,org2.account2`",
    ),
    **options,
) -> CommandResult:
    """
    Adds accounts to a release directive.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    ws.perform_action(
        package_id,
        EntityActions.RELEASE_DIRECTIVE_ADD_ACCOUNTS,
        release_directive=directive,
        target_accounts=target_accounts.split(","),
        release_channel=channel,
    )

    return MessageResult("Successfully added accounts to the release directive.")


@app.command("remove-accounts", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def release_directive_remove_accounts(
    directive: str = typer.Argument(
        show_default=False,
        help="Name of the release directive",
    ),
    channel: str = typer.Option(
        DEFAULT_CHANNEL,
        help="Name of the release channel to use",
    ),
    target_accounts: str = typer.Option(
        show_default=False,
        help="List of the accounts to remove from the release directive. Format must be `org1.account1,org2.account2`",
    ),
    **options,
) -> CommandResult:
    """
    Removes accounts from a release directive.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    ws.perform_action(
        package_id,
        EntityActions.RELEASE_DIRECTIVE_REMOVE_ACCOUNTS,
        release_directive=directive,
        target_accounts=target_accounts.split(","),
        release_channel=channel,
    )

    return MessageResult("Successfully removed accounts from the release directive.")
