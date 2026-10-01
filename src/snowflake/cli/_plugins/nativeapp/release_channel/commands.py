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
from snowflake.cli.api.commands.command_docs import (
    PUBLIC_PREVIEW_NO_GOV,
    CommandDocs,
    Example,
    Include,
    RelatedLink,
    code,
    link,
    note,
    plain_text,
)
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.entities.utils import EntityActions
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

_NATIVE_APP_RELATED = (
    link("/developer-guide/snowflake-cli/index"),
    link("/developer-guide/snowflake-cli/native-apps/overview"),
    link(
        "/developer-guide/snowflake-cli/command-reference/overview",
        "Snowflake CLI command reference",
    ),
    link(
        "/developer-guide/snowflake-cli/command-reference/native-apps-commands/overview"
    ),
    link(
        "/developer-guide/snowflake-cli/command-reference/native-apps-commands/publish-app"
    ),
)


def _release_channel_related(*extra: RelatedLink) -> tuple[RelatedLink, ...]:
    return (
        _NATIVE_APP_RELATED
        + (
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-directive/overview"
            ),
        )
        + extra
    )


_TEXT_SNOWCLI_RELEASE_CHANNELS_NOTE = Include(
    tag="TextSnowcliReleaseChannelsNote",
    path="INCLUDE/text/text-snow" + "cli-release-channels-note.mdx",
    help_content=(
        note(
            "The release channels feature might not be available in all regions. "
            "Please contact Snowflake Support for more information.",
        ),
    ),
)


@app.command(
    "list",
    requires_connection=True,
    docs=CommandDocs(
        related=_release_channel_related(),
        banners=(PUBLIC_PREVIEW_NO_GOV,),
        usage_notes=(
            _TEXT_SNOWCLI_RELEASE_CHANNELS_NOTE,
            plain_text(
                "The ",
                code("snow app release-channel list"),
                " lists all the release channels available in the current application package.\n",
                "If release channels are not enabled in the application package, this command returns no results.",
            ),
        ),
        examples=(
            Example(
                description=plain_text("List all the release channels:"),
                command="snow app release-channel list",
            ),
            Example(
                description=plain_text(
                    "To display the results in JSON format, add the ",
                    code("--format=json"),
                    " option:",
                ),
                command="snow app release-channel list --format=json",
            ),
        ),
    ),
)
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

    if cli_context.output_format.is_json:
        return CollectionResult(channels)


@app.command(
    "add-accounts",
    requires_connection=True,
    docs=CommandDocs(
        related=_release_channel_related(
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-channel/list"
            ),
        ),
        banners=(PUBLIC_PREVIEW_NO_GOV,),
        usage_notes=(
            _TEXT_SNOWCLI_RELEASE_CHANNELS_NOTE,
            plain_text(
                "The ",
                code("snow app release-channel add-accounts"),
                " command adds a list of accounts to an existing release channel for an application package.\n",
                "The release channel must already exist, and release channels must be enabled for the application package. Only non-default release channels can have accounts associated with them.\n",
                "To view the available release channels for the application package, use the ",
                link(
                    "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-channel/list",
                    "snow app release-channel list",
                ),
                " command.\n",
                "The specified accounts are provided in the format of ORGANIZATION_NAME.ACCOUNT_NAME and separated by comma.",
            ),
        ),
        examples=(
            Example(
                description=plain_text("Add accounts to the ALPHA release channel:"),
                command=(
                    "snow app release-channel add-accounts ALPHA "
                    "--target-accounts ORG1.ACCT1,ORG2.ACCT2"
                ),
            ),
        ),
    ),
)
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


@app.command(
    "remove-accounts",
    requires_connection=True,
    docs=CommandDocs(
        related=_release_channel_related(
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-channel/list"
            ),
        ),
        banners=(PUBLIC_PREVIEW_NO_GOV,),
        usage_notes=(
            _TEXT_SNOWCLI_RELEASE_CHANNELS_NOTE,
            plain_text(
                "The ",
                code("snow app release-channel remove-accounts"),
                " command removes a list of accounts from an existing release channel for an application package.\n",
                "The release channel must already exist, and release channels must be enabled for the application package. Only non-default release channels can have accounts associated with them.\n",
                "To view the available release channels for the application package, use the ",
                link(
                    "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-channel/list",
                    "snow app release-channel list",
                ),
                " command.\n",
                "The specified accounts are provided in the format of ORGANIZATION_NAME.ACCOUNT_NAME and separated by comma.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Remove accounts from the ALPHA release channel:"
                ),
                command=(
                    "snow app release-channel remove-accounts ALPHA "
                    "--target-accounts ORG1.ACCT1,ORG2.ACCT2"
                ),
            ),
        ),
    ),
)
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
@app.command(
    "set-accounts",
    requires_connection=True,
    docs=CommandDocs(
        related=_release_channel_related(
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-channel/list"
            ),
        ),
        banners=(PUBLIC_PREVIEW_NO_GOV,),
        usage_notes=(
            _TEXT_SNOWCLI_RELEASE_CHANNELS_NOTE,
            plain_text(
                "The ",
                code("snow app release-channel set-accounts"),
                " command assigns a list of accounts to an existing release channel of an application package.\n",
                "The release channel must already exist, and release channels must be enabled for the application package. Only non-default release channels can have accounts associated with them.\n\n",
                "To specify the accounts, provide comma-separated ORGANIZATION_NAME.ACCOUNT_NAME values.\n\n",
                "To view the available release channels for the application package, use the ",
                link(
                    "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-channel/list",
                    "snow app release-channel list",
                ),
                " command.",
            ),
        ),
        examples=(
            Example(
                description=plain_text("Set accounts for the ALPHA release channel:"),
                command=(
                    "snow app release-channel set-accounts ALPHA "
                    "--target-accounts ORG1.ACCT1,ORG2.ACCT2"
                ),
            ),
        ),
    ),
)
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


@app.command(
    "add-version",
    requires_connection=True,
    docs=CommandDocs(
        related=_release_channel_related(
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-channel/list"
            ),
        ),
        banners=(PUBLIC_PREVIEW_NO_GOV,),
        usage_notes=(
            _TEXT_SNOWCLI_RELEASE_CHANNELS_NOTE,
            plain_text(
                "The ",
                code("snow app release-channel add-version"),
                " command adds a version to an existing release channel for an application package.\n",
                "The release channel must already exist, and release channels must be enabled for the application package.\n",
                "To view the available release channels for the application package, use the ",
                link(
                    "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-channel/list",
                    "snow app release-channel list",
                ),
                " command.\n",
                "The specified version must already exist in the application package, and the version must not already be associated with the release channel.\n",
                "If the maximum number of versions is already associated with the release channel, the command fails.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Add version v1 to the default release channel:"
                ),
                command="snow app release-channel add-version --version v1 DEFAULT",
            ),
            Example(
                description=plain_text(
                    "Add version v1 to a non-default release channel:"
                ),
                command="snow app release-channel add-version --version v1 ALPHA",
            ),
        ),
    ),
)
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


@app.command(
    "remove-version",
    requires_connection=True,
    docs=CommandDocs(
        related=_release_channel_related(
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-channel/list"
            ),
        ),
        banners=(PUBLIC_PREVIEW_NO_GOV,),
        usage_notes=(
            _TEXT_SNOWCLI_RELEASE_CHANNELS_NOTE,
            plain_text(
                "The ",
                code("snow app release-channel remove-version"),
                " command removes a version from an existing release channel for an application package.\n",
                "The release channel must already exist, and release channels must be enabled for the application package.\n",
                "To view the available release channels for the application package, use the ",
                link(
                    "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-channel/list",
                    "snow app release-channel list",
                ),
                " command.\n",
                "The specified version must already exist in the application package, and the version must already be associated with the release channel.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Remove version v1 from the default release channel:"
                ),
                command="snow app release-channel remove-version --version v1 DEFAULT",
            ),
            Example(
                description=plain_text(
                    "Remove version v1 from a non-default release channel:"
                ),
                command="snow app release-channel remove-version --version v1 ALPHA",
            ),
        ),
    ),
)
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
