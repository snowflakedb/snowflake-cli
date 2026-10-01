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
from snowflake.cli._plugins.nativeapp.artifacts import VersionInfo
from snowflake.cli._plugins.nativeapp.v2_conversions.compat import (
    force_project_definition_v2,
)
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    bullet,
    bullet_list,
    code,
    link,
    note,
    plain_text,
    ref,
)
from snowflake.cli.api.commands.decorators import (
    with_project_definition,
)
from snowflake.cli.api.commands.flags import ForceOption, InteractiveOption
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.entities.utils import EntityActions
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    ObjectResult,
)
from snowflake.cli.api.project.util import to_identifier

app = SnowTyperFactory(
    name="version",
    help="Manages versions defined in an application package",
)

log = logging.getLogger(__name__)


@app.command(
    requires_connection=True,
    docs=CommandDocs(
        related=(
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
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/open-app"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/run-app"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/teardown-app"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/version/app-version-drop"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/version/app-version-list"
            ),
        ),
        usage_notes=(
            note(
                "This command does not accept a role or warehouse overrides to your ",
                code("config.toml"),
                " file. Please add them to the native app definition in the ",
                code("snowflake.yml"),
                " or ",
                code("snowflake.local.yml"),
                " instead.",
            ),
            plain_text(
                "This command creates an application package (if it does not exist) with a version and an optional patch."
            ),
            bullet_list(
                bullet(
                    "If you do not provide a version, the command uses the version specified in the ",
                    code("manifest.yml"),
                    " file. If the version is not present in the ",
                    code("manifest.yml"),
                    " file, the command throws an error.",
                ),
                bullet(
                    "If you provide both the version argument and the ",
                    code("--patch"),
                    " option, and the application package does not already exist, the command throws an error. You should only provide the version argument to create a new application package with the required version.",
                ),
                bullet(
                    "If you provide both the version argument and the ",
                    code("--patch"),
                    " option, and the version does not already exist, the command throws an error. You should only provide the version argument to create a new version with a predetermined patch 0.",
                ),
                bullet(
                    "If you are working in a Git repository and execute this command, the command checks for local changes to your working copy. If it finds local changes, it prompts you to confirm whether it is safe to proceed. You can skip this check using ",
                    code("--skip-git-check"),
                    " option.",
                ),
                bullet(
                    "If the application package does not exist, a new one is created by the ",
                    ref("sf-cli"),
                    " is tagged with a special comment ",
                    code("GENERATED_BY_SNOWCLI"),
                    ". It also runs any post-deploy hooks and uploads code files to the stage.",
                ),
                bullet(
                    "If the application package already exists and its distribution property is ",
                    code("INTERNAL"),
                    ", the command checks if the package was created by the ",
                    ref("sf-cli"),
                    ". If it was not, the command throws an error. If the distribution of the application package is ",
                    code("EXTERNAL"),
                    ", no such check is performed.",
                ),
                bullet(
                    "The command warns you if the application package you are working with has a different value for distribution than is set in your resolved project definition, but continues execution.",
                ),
                bullet(
                    "If the version is referenced in a release directive for the application package, the command prompts you to confirm whether you want to create a patch on this version.",
                ),
                bullet(
                    "If the version already exists and you do not provide a ",
                    code("--patch"),
                    " option, the Native Apps Framework automatically increments the patch number for this existing version. Else, it creates a custom patch under the version provided by you.",
                ),
                bullet(
                    "The ",
                    code("--label"),
                    " option sets a label for the version or patch created with this command. If specified, this value overrides the label specified for the ",
                    code("version"),
                    " defined in the application's ",
                    code("manifest.yml"),
                    " file.",
                ),
                bullet(
                    "If you specify a named version, such as ",
                    code("snow app version create my_version"),
                    ", the ",
                    code("version"),
                    " field in the ",
                    code("manifest.yml"),
                    " file is ignored.",
                ),
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "These examples assume you have made the necessary changes to your code files and added them to your ",
                    code("snowflake.yml"),
                    " or ",
                    code("snowflake.local.yml"),
                    " files.",
                ),
                command="",
            ),
            Example(
                description=plain_text(
                    "If you want to create an application package and add a version V1 to it, use the following command:"
                ),
                command='snow app version create V1 --connection="dev"',
            ),
            Example(
                description=plain_text(
                    "You can also use the command above to create a version V1 on an existing application package."
                ),
                command="",
            ),
            Example(
                description=plain_text(
                    "If you want to add a patch to version V1 using the auto-increment functionality and invoke the interactive mode, use the following command:"
                ),
                command='snow app version create V1 --interactive --connection="dev"',
            ),
            Example(
                description=plain_text(
                    "If you want to add a custom patch number to version ",
                    code("V1"),
                    " and bypass the interactive mode, even if you are in an interactive shell, use the following command:",
                ),
                command='snow app version create V1 --patch 42 --force --connection="dev"',
            ),
            Example(
                description=plain_text(
                    "To create a new version from the current content of the stage without syncing files to the stage first, use the following command:"
                ),
                command='snow app version create V1 --from-stage --connection="dev"',
            ),
        ),
    ),
)
@with_project_definition()
@force_project_definition_v2()
def create(
    version: Optional[str] = typer.Argument(
        None,
        help=f"""Version to define in your application package. If the version already exists, an auto-incremented patch is added to the version instead. Defaults to the version specified in the `manifest.yml` file.""",
    ),
    patch: Optional[int] = typer.Option(
        None,
        "--patch",
        help=f"""The patch number you want to create for an existing version.
        Defaults to undefined if it is not set, which means the Snowflake CLI either uses the patch specified in the `manifest.yml` file or automatically generates a new patch number.""",
    ),
    label: Optional[str] = typer.Option(
        None,
        "--label",
        help="A label for the version that is displayed to consumers. If unset, the version label specified in `manifest.yml` file is used.",
    ),
    skip_git_check: Optional[bool] = typer.Option(
        False,
        "--skip-git-check",
        help="When enabled, the Snowflake CLI skips checking if your project has any untracked or stages files in git. Default: unset.",
        is_flag=True,
    ),
    from_stage: bool = typer.Option(
        False,
        "--from-stage",
        help="When enabled, the Snowflake CLI creates a version from the current application package stage without syncing to the stage first.",
        is_flag=True,
    ),
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """
    Adds a new patch to the provided version defined in your application package. If the version does not exist, creates a version with patch 0.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    result: VersionInfo = ws.perform_action(
        package_id,
        EntityActions.VERSION_CREATE,
        version=version,
        patch=patch,
        label=label,
        force=force,
        interactive=interactive,
        skip_git_check=skip_git_check,
        from_stage=from_stage,
    )

    message = "Version create is now complete."
    if cli_context.output_format.is_json:
        return ObjectResult(
            {
                "message": message,
                "version": to_identifier(result.version_name),
                "patch": result.patch_number,
                "label": result.label,
            }
        )
    else:
        return MessageResult(message)


@app.command(
    "list",
    requires_connection=True,
    docs=CommandDocs(
        related=(
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
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/open-app"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/run-app"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/teardown-app"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/version/app-version-create"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/version/app-version-list"
            ),
        ),
        usage_notes=(
            note(
                "This command does not accept a role or warehouse overrides to your ",
                code("config.toml"),
                " file. Please add them to the native app definition in the ",
                code("snowflake.yml"),
                " or ",
                code("snowflake.local.yml"),
                " instead.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "This example assumes you have valid ",
                    code("snowflake.yml"),
                    " or ",
                    code("snowflake.local.yml"),
                    " project definition file(s).",
                ),
                command="",
            ),
            Example(
                description=plain_text(
                    "If you want to list all existing versions of an application package specified in your resolved project definition, use the following command:"
                ),
                command='snow app version list --connection="dev" --format JSON',
            ),
            Example(
                description=plain_text(
                    "This command displays the results in JSON format instead of the default TABLE format."
                ),
                command="",
            ),
        ),
    ),
)
@with_project_definition()
@force_project_definition_v2()
def version_list(
    **options,
) -> CommandResult:
    """
    Lists all versions defined in an application package.
    """
    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    cursor = ws.perform_action(
        package_id,
        EntityActions.VERSION_LIST,
    )
    return CollectionResult(cursor)


@app.command(
    requires_connection=True,
    docs=CommandDocs(
        related=(
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
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/open-app"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/run-app"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/teardown-app"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/version/app-version-create"
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/native-apps-commands/version/app-version-list"
            ),
        ),
        usage_notes=(
            note(
                "This command does not accept a role or warehouse overrides to your ",
                code("config.toml"),
                " file. Please add them to the native app definition in the ",
                code("snowflake.yml"),
                " or ",
                code("snowflake.local.yml"),
                " instead.",
            ),
            bullet_list(
                bullet(
                    "The command warns you if the application package you are working with has a different value for distribution than is set in your resolved project definition, but continues execution.",
                ),
                bullet(
                    "If you do not provide a version, the command uses the version specified in the ",
                    code("manifest.yml"),
                    " file. If the version is not present in the ",
                    code("manifest.yml"),
                    " file, the command throws an error.",
                ),
                bullet(
                    "If you want to drop a version that is referenced by a release directive, you must first set that release directive to a different version and then run this command.",
                ),
                bullet(
                    "Because this action is destructive, the command prompts you to confirm dropping the version before it proceeds. Use ",
                    code("--force"),
                    " option to bypass the prompt and drop the version.",
                ),
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "These examples assume you have valid ",
                    code("snowflake.yml"),
                    " or ",
                    code("snowflake.local.yml"),
                    " project definition file(s).",
                ),
                command="",
            ),
            Example(
                description=plain_text(
                    "If you want to drop an existing version V1 from your application package, use the following command:"
                ),
                command='snow app version drop V1 --connection="dev"',
            ),
            Example(
                description=plain_text(
                    "If you want to drop the version and invoke the interactive mode, use the following command:"
                ),
                command='snow app version drop V1 --interactive --connection="dev"',
            ),
            Example(
                description=plain_text(
                    "If you want to drop the version and bypass the interactive mode even if you are in an interactive shell, use the following command:"
                ),
                command='snow app version drop V1 --force --connection="dev"',
            ),
        ),
    ),
)
@with_project_definition()
@force_project_definition_v2()
def drop(
    version: Optional[str] = typer.Argument(
        None,
        help="Version defined in an application package that you want to drop. Defaults to the version specified in the `manifest.yml` file.",
    ),
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """
    Drops a version defined in your application package. Versions can either be passed in as an argument to the command or read from the `manifest.yml` file.
    Dropping patches is not allowed.
    """
    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    package_id = options["package_entity_id"]
    ws.perform_action(
        package_id,
        EntityActions.VERSION_DROP,
        version=version,
        interactive=interactive,
        force=force,
    )
    return MessageResult(f"Version drop is now complete.")
