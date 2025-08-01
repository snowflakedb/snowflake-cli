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

from typing import List

from snowflake.cli._plugins.dcm.dcm_project_entity_model import DCMProjectEntityModel
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.stage_path import StagePath


class DCMProjectManager(SqlExecutionMixin):
    def execute(
        self,
        project_name: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        dry_run: bool = False,
        alias: str | None = None,
        output_path: str | None = None,
    ):

        query = f"EXECUTE DCM PROJECT {project_name.sql_identifier}"
        if dry_run:
            query += " PLAN"
        else:
            query += " DEPLOY"
            if alias:
                query += f" AS {alias}"
        if configuration or variables:
            query += f" USING"
        if configuration:
            query += f" CONFIGURATION {configuration}"
        if variables:
            query += StageManager.parse_execute_variables(
                parse_key_value_variables(variables)
            ).removeprefix(" using")
        stage_path = StagePath.from_stage_str(from_stage)
        query += f" FROM {stage_path.absolute_path()}"
        if output_path:
            output_stage_path = StagePath.from_stage_str(output_path)
            query += f" OUTPUT_PATH {output_stage_path.absolute_path()}"
        return self.execute_query(query=query)

    def create(self, project: DCMProjectEntityModel) -> None:
        query = f"CREATE DCM PROJECT {project.fqn.sql_identifier}"
        self.execute_query(query)

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

    def list_versions(self, project_name: FQN):
        query = f"SHOW VERSIONS IN DCM PROJECT {project_name.identifier}"
        return self.execute_query(query=query)

    def drop_deployment(
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
