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
from textwrap import dedent
from typing import List, Optional

import typer
import yaml
from snowflake.cli._plugins.nativeapp.artifacts import BundleMap
from snowflake.cli._plugins.nativeapp.common_flags import (
    ForceOption,
    ValidateOption,
)
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.entities.common import EntityActions
from snowflake.cli.api.exceptions import IncompatibleParametersError
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.api.project.definition_conversion import (
    convert_project_definition_to_v2,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.secure_path import SecurePath

ws = SnowTyper(
    name="ws",
    help="Deploy and interact with snowflake.yml-based entities.",
)
log = logging.getLogger(__name__)


@ws.command()
def migrate(
    accept_templates: bool = typer.Option(
        False, "-t", "--accept-templates", help="Allows the migration of templates."
    ),
    **options,
):
    """Migrates the Snowpark and Streamlit project definition files from V1 to V2."""
    pd = DefinitionManager().unrendered_project_definition

    if pd.meets_version_requirement("2"):
        return MessageResult("Project definition is already at version 2.")

    pd_v2 = convert_project_definition_to_v2(pd, accept_templates)

    SecurePath("snowflake.yml").rename("snowflake_V1.yml")
    with open("snowflake.yml", "w") as file:
        yaml.dump(
            pd_v2.model_dump(
                exclude_unset=True, exclude_none=True, mode="json", by_alias=True
            ),
            file,
        )
    return MessageResult("Project definition migrated to version 2.")


@ws.command(requires_connection=True, hidden=True)
@with_project_definition()
def validate(
    **options,
):
    """Validates the project definition file."""
    # If we get to this point, @with_project_definition() has already validated the PDF schema
    return MessageResult("Project definition is valid.")


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


@ws.command(requires_connection=True)
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
            Paths, relative to the the project root, of files or directories you want to upload to a stage. If a file is
            specified, it must match one of the artifacts src pattern entries in snowflake.yml. If a directory is
            specified, it will be searched for subfolders or files to deploy based on artifacts src pattern entries. If
            unspecified, the command syncs all local changes to the stage."""
        ).strip(),
    ),
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
    )
    return MessageResult("Deployed successfully.")


@ws.command(requires_connection=True)
@with_project_definition()
def drop(
    entity_id: str = typer.Option(
        help=f"""The ID of the entity you want to drop.""",
    ),
    # TODO The following options should be generated automatically, depending on the specified entity type
    force: Optional[bool] = ForceOption,
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
    )
