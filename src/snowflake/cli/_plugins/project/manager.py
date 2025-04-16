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

from snowflake.cli._plugins.project.project_entity_model import ProjectEntityModel
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


class ProjectManager(SqlExecutionMixin):
    def execute(
        self,
        project_name: FQN,
        version: str | None = None,
        variables: List[str] | None = None,
        dry_run: bool = False,
    ):
        query = f"EXECUTE PROJECT {project_name.sql_identifier}"
        if variables:
            query += StageManager.parse_execute_variables(
                parse_key_value_variables(variables)
            )
        if version:
            query += f" WITH VERSION {version}"
        if dry_run:
            query += " DRY_RUN=TRUE"
        return self.execute_query(query=query)

    def _create_object(self, project_name: FQN) -> SnowflakeCursor:
        query = dedent(f"CREATE PROJECT {project_name.sql_identifier}")
        return self.execute_query(query)

    def create(
        self, project: ProjectEntityModel, initialize_version_from_local_files: bool
    ) -> None:
        self._create_object(project.fqn)
        if initialize_version_from_local_files:
            self.add_version(project=project)

    def _create_version(
        self,
        project_name: FQN,
        from_stage: str,
        alias: str | None = None,
        comment: str | None = None,
    ):
        stage_path = StagePath.from_stage_str(from_stage)
        query = f"ALTER PROJECT {project_name.identifier} ADD VERSION"
        if alias:
            query += f' IF NOT EXISTS "{alias}"'
        query += f" FROM {stage_path.absolute_path(at_prefix=True)}"
        if comment:
            query += f" COMMENT = '{comment}'"
        return self.execute_query(query=query)

    def add_version(
        self,
        project: ProjectEntityModel,
        prune: bool = False,
        from_stage: Optional[str] = None,
        alias: Optional[str] = None,
        comment: Optional[str] = None,
    ):
        """
        Adds a version to project. If [from_stage] is not defined,
        uploads local files to the stage defined in project definition.
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

        with cli_console.phase(f"Creating project version from stage {from_stage}"):
            return self._create_version(
                project_name=project.fqn,
                from_stage=from_stage,  # type:ignore
                alias=alias,
                comment=comment,
            )

    def list_versions(self, project_name: FQN):
        query = f"SHOW VERSIONS IN PROJECT {project_name.identifier}"
        return self.execute_query(query=query)
