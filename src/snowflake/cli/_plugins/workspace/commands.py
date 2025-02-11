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
from io import StringIO
from pathlib import Path
from textwrap import dedent
from typing import List, Optional

import typer
import yaml
from snowflake.cli._plugins.nativeapp.common_flags import (
    ForceOption,
    InteractiveOption,
    ValidateOption,
)
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.entities.utils import EntityActions
from snowflake.cli.api.exceptions import IncompatibleParametersError
from snowflake.cli.api.output.types import CollectionResult, MessageResult

ws = SnowTyperFactory(
    name="ws",
    help="Deploy and interact with snowflake.yml-based entities.",
    is_hidden=lambda: True,
)
log = logging.getLogger(__name__)


@ws.command(requires_connection=False, hidden=True)
@with_project_definition()
def dump(**options):
    """
    Dumps the project definition.
    """
    cli_context = get_cli_context()
    pd = cli_context.project_definition
    io = StringIO()
    yaml.safe_dump(
        pd.model_dump(mode="json", by_alias=True),
        io,
        sort_keys=False,
        width=float("inf"),  # Don't break lines
    )
    return MessageResult(io.getvalue())


@ws.command(requires_connection=True, hidden=True)
@with_project_definition()
def bundle(
    entity_id: str = typer.Option(
        help=f"""The ID of the entity you want to bundle.""",
    ),
    **options,
):
    """
    Prepares a local folder with the configured artifacts of the specified entity.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )

    bundle_map: BundleMap = ws.perform_action(entity_id, EntityActions.BUNDLE)
    return MessageResult(f"Bundle generated at {bundle_map.deploy_root()}")


@ws.command(requires_connection=True, hidden=True)
@with_project_definition()
def deploy(
    entity_id: str = typer.Option(
        help=f"""The ID of the entity you want to deploy.""",
    ),
    # TODO The following options should be generated automatically, depending on the specified entity type
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
):
    """
    Deploys the specified entity.
    """
    if prune is None and recursive is None and not paths:
        prune = True
        recursive = True
    else:
        if prune is None:
            prune = False
        if recursive is None:
            recursive = False

    if paths and prune:
        raise IncompatibleParametersError(["paths", "--prune"])

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )

    ws.perform_action(
        entity_id,
        EntityActions.DEPLOY,
        prune=prune,
        recursive=recursive,
        paths=paths,
        validate=validate,
        from_release_directive=from_release_directive,
        interactive=interactive,
        force=force,
    )
    return MessageResult("Deployed successfully.")


@ws.command(requires_connection=True, hidden=True)
@with_project_definition()
def drop(
    entity_id: str = typer.Option(
        help=f"""The ID of the entity you want to drop.""",
    ),
    # TODO The following options should be generated automatically, depending on the specified entity type
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    cascade: Optional[bool] = typer.Option(
        None,
        help=f"""Whether to drop all application objects owned by the application within the account. Default: false.""",
        show_default=False,
    ),
    **options,
):
    """
    Drops the specified entity.
    """
    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )

    ws.perform_action(
        entity_id,
        EntityActions.DROP,
        force_drop=force,
        interactive=interactive,
        cascade=cascade,
    )


@ws.command(requires_connection=True, hidden=True)
@with_project_definition()
def validate(
    entity_id: str = typer.Option(
        help=f"""The ID of the entity you want to validate.""",
    ),
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
):
    """Validates the specified entity."""
    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )

    ws.perform_action(
        entity_id,
        EntityActions.VALIDATE,
        interactive=interactive,
        force=force,
    )


version = SnowTyperFactory(
    name="version",
    help="Manages versions for project entities.",
)
ws.add_typer(version)


@version.command(name="list", requires_connection=True, hidden=True)
@with_project_definition()
def version_list(
    entity_id: str = typer.Option(
        help="The ID of the entity you want to list versions for.",
    ),
    **options,
):
    """Lists the versions of the specified entity."""
    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    cursor = ws.perform_action(
        entity_id,
        EntityActions.VERSION_LIST,
    )
    return CollectionResult(cursor)


@version.command(name="create", requires_connection=True, hidden=True)
@with_project_definition()
def version_create(
    entity_id: str = typer.Option(
        help="The ID of the entity you want to create a version for.",
    ),
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
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
):
    """Creates a new version for the specified entity."""

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    ws.perform_action(
        entity_id,
        EntityActions.VERSION_CREATE,
        version=version,
        patch=patch,
        label=label,
        skip_git_check=skip_git_check,
        interactive=interactive,
        force=force,
        from_stage=False,
    )


@version.command(name="drop", requires_connection=True, hidden=True)
@with_project_definition()
def version_drop(
    entity_id: str = typer.Option(
        help="The ID of the entity you want to create a version for.",
    ),
    version: Optional[str] = typer.Argument(
        None,
        help=f"""Version to define in your application package. If the version already exists, an auto-incremented patch is added to the version instead. Defaults to the version specified in the `manifest.yml` file.""",
    ),
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
):
    """
    Drops a version defined for your entity. Versions can either be passed in as an argument to the command or read from the `manifest.yml` file.
    Dropping patches is not allowed.
    """

    cli_context = get_cli_context()
    ws = WorkspaceManager(
        project_definition=cli_context.project_definition,
        project_root=cli_context.project_root,
    )
    ws.perform_action(
        entity_id,
        EntityActions.VERSION_DROP,
        version=version,
        interactive=interactive,
        force=force,
    )
