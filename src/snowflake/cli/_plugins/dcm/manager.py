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
from textwrap import dedent
from typing import List, Optional

from snowflake.cli._plugins.dcm.dcm_project_entity_model import DCMProjectEntityModel
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.artifacts.upload import sync_artifacts_with_stage
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.project_paths import ProjectPaths
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector.cursor import SnowflakeCursor


class DCMProjectManager(SqlExecutionMixin):
    def execute(
        self,
        project_name: FQN,
        configuration: str | None = None,
        version: str | None = None,
        from_stage: str | None = None,
        variables: List[str] | None = None,
        dry_run: bool = False,
    ):
        query = f"EXECUTE DCM PROJECT {project_name.sql_identifier}"
        if dry_run:
            query += " PLAN"
        else:
            query += " DEPLOY"
        if configuration or variables:
            query += f" USING"
        if configuration:
            query += f" CONFIGURATION {configuration}"
        if variables:
            query += StageManager.parse_execute_variables(
                parse_key_value_variables(variables)
            ).removeprefix(" using")
        if version:
            query += f" WITH VERSION {version}"
        elif from_stage:
            stage_path = StagePath.from_stage_str(from_stage)
            query += f" FROM {stage_path.absolute_path()}"
        return self.execute_query(query=query)

    def _create_object(self, project_name: FQN) -> SnowflakeCursor:
        query = dedent(f"CREATE DCM PROJECT {project_name.sql_identifier}")
        return self.execute_query(query)

    def create(self, project: DCMProjectEntityModel) -> None:
        self._create_object(project.fqn)

    def _create_version(
        self,
        project_name: FQN,
        from_stage: str,
        alias: str | None = None,
        comment: str | None = None,
    ):
        stage_path = StagePath.from_stage_str(from_stage)
        query = f"ALTER DCM PROJECT {project_name.identifier} ADD VERSION"
        if alias:
            query += f" IF NOT EXISTS {alias}"
        query += f" FROM {stage_path.absolute_path(at_prefix=True)}"
        if comment:
            query += f" COMMENT = '{comment}'"
        return self.execute_query(query=query)

    def add_version(
        self,
        project: DCMProjectEntityModel,
        prune: bool = False,
        from_stage: Optional[str] = None,
        alias: Optional[str] = None,
        comment: Optional[str] = None,
    ):
        """
        Adds a version to DCM Project. If [from_stage] is not defined,
        uploads local files to the stage defined in DCM Project definition.
        """

        if not from_stage:
            cli_context = get_cli_context()
            from_stage = project.stage
            with cli_console.phase("Uploading artifacts"):
                sync_artifacts_with_stage(
                    project_paths=ProjectPaths(project_root=cli_context.project_root),
                    stage_root=from_stage,
                    artifacts=project.artifacts,
                    prune=prune,
                )

        with cli_console.phase(f"Creating DCM Project version from stage {from_stage}"):
            return self._create_version(
                project_name=project.fqn,
                from_stage=from_stage,  # type:ignore
                alias=alias,
                comment=comment,
            )

    def list_versions(self, project_name: FQN):
        query = f"SHOW VERSIONS IN DCM PROJECT {project_name.identifier}"
        return self.execute_query(query=query)

    def drop_version(
        self,
        project_name: FQN,
        version_name: str,
        if_exists: bool = False,
    ):
        """
        Drops a version from the DCM Project.
        """
        query = f"ALTER DCM PROJECT {project_name.identifier} DROP VERSION"
        if if_exists:
            query += " IF EXISTS"
        query += f" {version_name}"
        return self.execute_query(query=query)
