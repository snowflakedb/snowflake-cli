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

import typer
import yaml
from click import ClickException
from snowflake.cli._plugins.nativeapp.artifacts import BundleMap
from snowflake.cli._plugins.snowpark.commands import migrate_v1_snowpark_to_v2
from snowflake.cli._plugins.streamlit.commands import migrate_v1_streamlit_to_v2
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.entities.common import EntityActions
from snowflake.cli.api.output.types import MessageResult
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
    """Migrates the Snowpark and Streamlit project definition files form V1 to V2."""
    pd = DefinitionManager().unrendered_project_definition

    if pd.meets_version_requirement("2"):
        return MessageResult("Project definition is already at version 2.")

    if "<% ctx." in str(pd):
        if not accept_templates:
            raise ClickException(
                "Project definition contains templates. They may not be migrated correctly, and require manual migration."
                "You can try again with --accept-templates  option, to attempt automatic migration."
            )
        log.warning(
            "Your V1 definition contains templates. We cannot guarantee the correctness of the migration."
        )

    if pd.streamlit:
        pd_v2 = migrate_v1_streamlit_to_v2(pd)
    elif pd.snowpark:
        pd_v2 = migrate_v1_snowpark_to_v2(pd)
    else:
        raise ValueError(
            "Only Snowpark and Streamlit entities are supported for migration."
        )

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
