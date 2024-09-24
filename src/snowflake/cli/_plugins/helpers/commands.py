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

import typer
import yaml
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.api.project.definition_conversion import (
    convert_project_definition_to_v2,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.secure_path import SecurePath

app = SnowTyperFactory(
    name="helpers",
    help="Helper commands.",
)


@app.command()
def v1_to_v2(
    accept_templates: bool = typer.Option(
        False, "-t", "--accept-templates", help="Allows the migration of templates."
    ),
    **options,
):
    """Migrates the Snowpark, Streamlit, and Native App project definition files from V1 to V2."""
    manager = DefinitionManager()
    pd = manager.unrendered_project_definition

    if pd.meets_version_requirement("2"):
        return MessageResult("Project definition is already at version 2.")

    pd_v2 = convert_project_definition_to_v2(manager.project_root, pd, accept_templates)

    SecurePath("snowflake.yml").rename("snowflake_V1.yml")
    with open("snowflake.yml", "w") as file:
        yaml.dump(
            pd_v2.model_dump(
                exclude_unset=True, exclude_none=True, mode="json", by_alias=True
            ),
            file,
            width=float("inf"),  # Don't break lines
        )
    return MessageResult("Project definition migrated to version 2.")
