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
from snowflake.cli.api.commands.command_docs import (
    PUBLIC_PREVIEW_NO_GOV,
    CommandDocs,
    Example,
    RelatedLink,
    bullet,
    bullet_list,
    code,
    link,
    plain_text,
)
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


def _release_directive_related(*extra: RelatedLink) -> tuple[RelatedLink, ...]:
    return (
        _NATIVE_APP_RELATED
        + (
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-channel/overview"
            ),
            link("/developer-guide/snowflake-cli/native-apps/publish-app"),
        )
        + extra
    )


@app.command(
    "list",
    requires_connection=True,
    docs=CommandDocs(
        related=_release_directive_related(),
        usage_notes=(
            plain_text(
                "The ",
                code("snow app release-directive list"),
                " command lists all the release directives available in the current application package.\n",
                "If no release channel is specified, release directives for all channels are listed. If a release channel is specified, only release directives for that channel are listed. If ",
                code("--like"),
                " is provided, only release directives matching the SQL pattern are listed.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "List all release directives associated with all release channels in an application package:"
                ),
                command="snow app release-directive list",
            ),
            Example(
                description=plain_text(
                    "List all release directives associated with a specific release channel in an application package:"
                ),
                command="snow app release-directive list --channel ALPHA",
            ),
            Example(
                description=plain_text(
                    "List all release directives starting with the word ",
                    code("vip"),
                    ":",
                ),
                command="snow app release-directive list --like vip%",
            ),
        ),
    ),
)
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


@app.command(
    "set",
    requires_connection=True,
    docs=CommandDocs(
        related=_release_directive_related(),
        usage_notes=(
            plain_text(
                "The ",
                code("snow app release-directive set"),
                " command sets the release directive for an application package.\n",
                "There are two types of release directives: default and custom.",
            ),
            bullet_list(
                bullet(
                    "When you set the default release directive, target accounts are not accepted."
                ),
                bullet(
                    "When you set a new custom release directive, the target accounts are required."
                ),
                bullet(
                    "When you update an existing custom release directive, the target accounts are optional."
                ),
            ),
            plain_text(
                "Target accounts are provided in the format ORGANIZATION_NAME.ACCOUNT_NAME, separated by commas.\n\n",
                "When release channels are enabled in the application package, the release directive is scoped to the specified release channel; otherwise, it is scoped to the application package.\n\n",
                "Snowflake recommends using the ",
                link(
                    "/developer-guide/snowflake-cli/command-reference/native-apps-commands/publish-app",
                    "snow app publish",
                ),
                " command to publish the application package and using the ",
                code("snow app release-directive set"),
                " command for creating custom release directives.\n",
                "See ",
                link("/developer-guide/snowflake-cli/native-apps/publish-app"),
                " for more information.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Set the default release directive for an application package:"
                ),
                command="snow app release-directive set DEFAULT --version v1 --patch 1",
            ),
            Example(
                description=plain_text(
                    "Set a custom release directive for an application package:"
                ),
                command=(
                    "snow app release-directive set CUSTOM_DIR --version v1 --patch 1 "
                    "--target-accounts ORG1.ACCT1,ORG2.ACCT2"
                ),
            ),
            Example(
                description=plain_text(
                    "Update an existing custom release directive for an application package:"
                ),
                command="snow app release-directive set CUSTOM_DIR --version v1 --patch 2",
            ),
            Example(
                description=plain_text(
                    "Set the default release directive of a release channel when the application package has release channels enabled:"
                ),
                command=(
                    "snow app release-directive set DEFAULT --version v1 --patch 1 "
                    "--channel ALPHA"
                ),
            ),
        ),
    ),
)
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


@app.command(
    "unset",
    requires_connection=True,
    docs=CommandDocs(
        related=_release_directive_related(),
        usage_notes=(
            plain_text(
                "The ",
                code("snow app release-directive unset"),
                " command removes a custom release directive from an application package.\n",
                "The specified release directive must already exist in the application package.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Remove the custom release directive ",
                    code("my_directive"),
                    " from the application package:\n\nWhen release channels are enabled, release directives become part of a release channel.",
                ),
                command="snow app release-directive unset my_directive",
            ),
            Example(
                description=plain_text(
                    "Remove the custom ",
                    code("special_alpha_directive"),
                    " release directive associated with release channel ",
                    code("ALPHA"),
                    ":",
                ),
                command=(
                    "snow app release-directive unset special_alpha_directive "
                    "--channel ALPHA"
                ),
            ),
        ),
    ),
)
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


@app.command(
    "add-accounts",
    requires_connection=True,
    docs=CommandDocs(
        related=_release_directive_related(),
        usage_notes=(
            plain_text(
                "The ",
                code("snow app release-directive add-accounts"),
                " command adds a list of accounts to an existing custom release directive for an application package.\n",
                "The custom release directive must already exist in the application package (or the release channel if enabled).\n\n",
                "To specify the accounts, provide comma-separated values in the format ORGANIZATION_NAME.ACCOUNT_NAME.\n\n",
                "To view the available release directives for the application package, use the ",
                link(
                    "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-directive/list",
                    "snow app release-directive list",
                ),
                " command.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "To add accounts to the ",
                    code("my_directive"),
                    " custom release directive:",
                ),
                command=(
                    "snow app release-directive add-accounts my_directive "
                    "--target-accounts ORG1.ACCT1,ORG2.ACCT2"
                ),
            ),
            Example(
                description=plain_text(
                    "When release channels are enabled, release directives become part of a release channel. To add accounts to the ",
                    code("special_alpha_directive"),
                    " custom release directive associated with release channel ",
                    code("ALPHA"),
                    ":",
                ),
                command=(
                    "snow app release-directive add-accounts special_alpha_directive "
                    "--channel ALPHA --target-accounts ORG1.ACCT1,ORG2.ACCT2"
                ),
            ),
        ),
    ),
)
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


@app.command(
    "remove-accounts",
    requires_connection=True,
    docs=CommandDocs(
        related=_release_directive_related(),
        banners=(PUBLIC_PREVIEW_NO_GOV,),
        usage_notes=(
            plain_text(
                "The ",
                code("snow app release-directive remove-accounts"),
                " command removes a list of accounts from an existing custom release directive for an application package.\n",
                "The specified release directive must already exist in the application package (or the release channel if enabled).\n\n",
                "To specify the accounts, provide comma-separated ORGANIZATION_NAME.ACCOUNT_NAME values.\n\n",
                "To view the available release directives for the application package, use the ",
                link(
                    "/developer-guide/snowflake-cli/command-reference/native-apps-commands/release-directive/list",
                    "snow app release-directive list",
                ),
                " command.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Remove accounts from the ",
                    code("my_directive"),
                    " custom release directive:",
                ),
                command=(
                    "snow app release-directive remove-accounts my_directive "
                    "--target-accounts ORG1.ACCT1,ORG2.ACCT2"
                ),
            ),
            Example(
                description=plain_text(
                    "When release channels are enabled, release directives become part of a release channel. To remove accounts from the ",
                    code("special_alpha_directive"),
                    " custom release directive associated with release channel ",
                    code("ALPHA"),
                    ":",
                ),
                command=(
                    "snow app release-directive remove-accounts special_alpha_directive "
                    "--channel ALPHA --target-accounts ORG1.ACCT1,ORG2.ACCT2"
                ),
            ),
        ),
    ),
)
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
