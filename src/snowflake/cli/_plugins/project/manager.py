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

from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin


class ProjectManager(SqlExecutionMixin):
    def execute(
        self, project_name: FQN, version: str | None = None, dry_run: bool = False
    ):
        query = f"EXECUTE PROJECT {project_name.sql_identifier}"
        if dry_run:
            query += " DRY_RUN=TRUE"
        return self._execute_query(query=query)

    def create(
        self,
        project_name: FQN,
    ) -> str:
        queries = dedent(f"create project if not exist {project_name.sql_identifier}")
        return self._execute_queries(queries=queries)

    def create_version(self, project_name: FQN, stage_name: FQN):
        query = f"ALTER PROJECT {project_name.sql_identifier} ADD VERSION FROM {stage_name.sql_identifier}"
        return self._execute_query(query=query)
