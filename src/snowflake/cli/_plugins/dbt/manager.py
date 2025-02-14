# Copyright (c) 2025 Snowflake Inc.
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

from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class DBTManager(SqlExecutionMixin):
    def list(self) -> SnowflakeCursor:  # noqa: A003
        query = "SHOW DBT"
        return self.execute_query(query)

    def execute(self, dbt_command: str, name: str, *dbt_cli_args):
        query = f"EXECUTE DBT {name} {dbt_command}"
        if dbt_cli_args:
            query += " " + " ".join([arg for arg in dbt_cli_args])
        return self.execute_query(query)
