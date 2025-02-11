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
from snowflake.cli._plugins.nativeapp.v2_conversions.compat import (
    force_project_definition_v2,
)
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.entities.utils import EntityActions
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
)

app = SnowTyperFactory(
    name="release-channel",
    help="Manages release channels of an application package",
)

log = logging.getLogger(__name__)


@app.command("list", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def release_channel_list(
    channel: Optional[str] = typer.Argument(
        default=None,
        show_default=False,
        help="The release channel to list. If not provided, all release channels are listed.",
    ),
    **options,
) -> CommandResult:
    """
    Lists the release channels available for an application package.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    channels = ws.perform_action(
        package_id,
        EntityActions.RELEASE_CHANNEL_LIST,
        release_channel=channel,
    )

    if cli_context.output_format == OutputFormat.JSON:
        return CollectionResult(channels)


@app.command("add-accounts", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def release_channel_add_accounts(
    channel: str = typer.Argument(
        show_default=False,
        help="The release channel to add accounts to.",
    ),
    target_accounts: str = typer.Option(
        show_default=False,
        help="The accounts to add to the release channel. Format must be `org1.account1,org2.account2`.",
    ),
    **options,
) -> CommandResult:
    """
    Adds accounts to a release channel.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    ws.perform_action(
        package_id,
        EntityActions.RELEASE_CHANNEL_ADD_ACCOUNTS,
        release_channel=channel,
        target_accounts=target_accounts.split(","),
    )

    return MessageResult("Successfully added accounts to the release channel.")


@app.command("remove-accounts", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def release_channel_remove_accounts(
    channel: str = typer.Argument(
        show_default=False,
        help="The release channel to remove accounts from.",
    ),
    target_accounts: str = typer.Option(
        show_default=False,
        help="The accounts to remove from the release channel. Format must be `org1.account1,org2.account2`.",
    ),
    **options,
) -> CommandResult:
    """
    Removes accounts from a release channel.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    ws.perform_action(
        package_id,
        EntityActions.RELEASE_CHANNEL_REMOVE_ACCOUNTS,
        release_channel=channel,
        target_accounts=target_accounts.split(","),
    )

    return MessageResult("Successfully removed accounts from the release channel.")


@with_project_definition()
@app.command("set-accounts", requires_connection=True)
@force_project_definition_v2()
def release_channel_set_accounts(
    channel: str = typer.Argument(
        show_default=False,
        help="The release channel to set accounts for.",
    ),
    target_accounts: str = typer.Option(
        show_default=False,
        help="The accounts to set for the release channel. Format must be `org1.account1,org2.account2`.",
    ),
    **options,
) -> CommandResult:
    """
    Sets accounts for a release channel.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    ws.perform_action(
        package_id,
        EntityActions.RELEASE_CHANNEL_SET_ACCOUNTS,
        release_channel=channel,
        target_accounts=target_accounts.split(","),
    )

    return MessageResult("Successfully set accounts for the release channel.")


@app.command("add-version", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def release_channel_add_version(
    channel: str = typer.Argument(
        show_default=False,
        help="The release channel to add a version to.",
    ),
    version: str = typer.Option(
        show_default=False,
        help="The version to add to the release channel.",
    ),
    **options,
) -> CommandResult:
    """
    Adds a version to a release channel.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    ws.perform_action(
        package_id,
        EntityActions.RELEASE_CHANNEL_ADD_VERSION,
        release_channel=channel,
        version=version,
    )

    return MessageResult(
        f"Successfully added version {version} to the release channel."
    )


@app.command("remove-version", requires_connection=True)
@with_project_definition()
@force_project_definition_v2()
def release_channel_remove_version(
    channel: str = typer.Argument(
        show_default=False,
        help="The release channel to remove a version from.",
    ),
    version: str = typer.Option(
        show_default=False,
        help="The version to remove from the release channel.",
    ),
    **options,
) -> CommandResult:
    """
    Removes a version from a release channel.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    ws.perform_action(
        package_id,
        EntityActions.RELEASE_CHANNEL_REMOVE_VERSION,
        release_channel=channel,
        version=version,
    )

    return MessageResult(
        f"Successfully removed version {version} from the release channel."
    )
