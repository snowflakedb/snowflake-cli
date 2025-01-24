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
from typing import List

from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin


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

    def create(
        self,
        project_name: FQN,
    ) -> str:
        queries = dedent(f"CREATE PROJECT IF NOT EXISTS {project_name.sql_identifier}")
        return self.execute_query(query=queries)

    def create_version(self, project_name: FQN, stage_name: FQN):
        query = f"ALTER PROJECT {project_name.identifier} ADD VERSION FROM @{stage_name.identifier}"
        return self.execute_query(query=query)
