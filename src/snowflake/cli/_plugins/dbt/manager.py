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

from typing import Optional

import yaml
from click import ClickException
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class DBTManager(SqlExecutionMixin):
    def list(self) -> SnowflakeCursor:  # noqa: A003
        query = "SHOW DBT PROJECTS"
        return self.execute_query(query)

    def deploy(
        self,
        path: SecurePath,
        name: FQN,
        dbt_version: Optional[str],
        dbt_adapter_version: str,
        execute_in_warehouse: Optional[str],
        force: bool,
    ) -> SnowflakeCursor:
        dbt_project_path = path / "dbt_project.yml"
        if not dbt_project_path.exists():
            raise ClickException(f"dbt_project.yml does not exist in provided path.")

        if dbt_version is None:
            with dbt_project_path.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
                dbt_project_config = yaml.safe_load(fd)
                try:
                    dbt_version = dbt_project_config["version"]
                except (KeyError, TypeError):
                    raise ClickException(
                        f"dbt-version was not provided and is not available in dbt_project.yml"
                    )

        with cli_console.phase("Creating temporary stage"):
            stage_manager = StageManager()
            stage_fqn = FQN.from_string(f"dbt_{name}_stage").using_context()
            stage_name = stage_manager.get_standard_stage_prefix(stage_fqn)
            stage_manager.create(stage_fqn, temporary=True)

        with cli_console.phase("Copying project files to stage"):
            results = list(stage_manager.put_recursive(path.path, stage_name))
            cli_console.step(f"Copied {len(results)} files")

        with cli_console.phase("Creating DBT project"):
            query = f"""{'CREATE OR REPLACE' if force is True else 'CREATE'} DBT PROJECT {name}
FROM {stage_name}
DBT_VERSION='{dbt_version}'"""

            if dbt_adapter_version:
                query += f"\nDBT_ADAPTER_VERSION='{dbt_adapter_version}'"
            if execute_in_warehouse:
                query += f"\nWAREHOUSE='{execute_in_warehouse}'"
            return self.execute_query(query)

    def execute(self, dbt_command: str, name: str, run_async: bool, *dbt_cli_args):
        if dbt_cli_args:
            dbt_command = dbt_command + " " + " ".join([arg for arg in dbt_cli_args])
        query = f"EXECUTE DBT PROJECT {name} args='{dbt_command.strip()}'"
        return self.execute_query(query, _exec_async=run_async)
