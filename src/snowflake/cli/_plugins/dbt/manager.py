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

from pathlib import Path

from click import ClickException
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class DBTManager(SqlExecutionMixin):
    def list(self) -> SnowflakeCursor:  # noqa: A003
        query = "SHOW DBT"
        return self.execute_query(query)

    def deploy(self, path: Path, name: FQN, force: bool) -> SnowflakeCursor:
        # TODO: what to do with force?
        if not path.joinpath("dbt_project.yml").exists():
            raise ClickException(f"dbt_project.yml does not exist in provided path.")

        with cli_console.phase("Creating temporary stage"):
            stage_manager = StageManager()
            stage_fqn = FQN.from_string(f"dbt_{name}_stage").using_context()
            stage_name = stage_manager.get_standard_stage_prefix(stage_fqn)
            stage_manager.create(stage_fqn, temporary=True)

        with cli_console.phase("Copying project files to stage"):
            results = list(stage_manager.put_recursive(path, stage_name))
            cli_console.step(f"Copied {len(results)} files")

        with cli_console.phase("Creating DBT project"):
            query = f"CREATE OR REPLACE DBT {name} FROM {stage_name}"
            return self.execute_query(query)

    def execute(self, dbt_command: str, name: str, *dbt_cli_args):
        query = f"EXECUTE DBT {name} {dbt_command}"
        if dbt_cli_args:
            query += " " + " ".join([arg for arg in dbt_cli_args])
        return self.execute_query(query)
