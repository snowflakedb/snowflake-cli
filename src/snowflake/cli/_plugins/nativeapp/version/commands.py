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
from snowflake.cli._plugins.nativeapp.common_flags import ForceOption, InteractiveOption
from snowflake.cli._plugins.nativeapp.v2_conversions.compat import (
    force_project_definition_v2,
)
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import (
    with_project_definition,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.entities.utils import EntityActions
from snowflake.cli.api.output.formats import OutputFormat
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


@app.command(requires_connection=True)
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
    if cli_context.output_format == OutputFormat.JSON:
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


@app.command("list", requires_connection=True)
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


@app.command(requires_connection=True)
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
