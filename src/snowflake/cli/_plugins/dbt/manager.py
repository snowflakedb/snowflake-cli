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
from typing import Optional

import yaml
from click import ClickException
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class StdoutExecutionMixin(SqlExecutionMixin):
    def execute_query(self, query, **kwargs):
        if FeatureFlag.ENABLE_DBT_POC.is_enabled():
            from unittest.mock import MagicMock

            cli_console.message(f"Sending query: {query}")
            mock_cursor = MagicMock()
            mock_cursor.description = []
            return mock_cursor
        return super().execute_query(query, **kwargs)


class DBTManager(StdoutExecutionMixin):
    def list(self) -> SnowflakeCursor:  # noqa: A003
        query = "SHOW DBT PROJECT"
        return self.execute_query(query)

    def deploy(
        self,
        path: Path,
        name: FQN,
        dbt_version: Optional[str],
        dbt_adapter_version: str,
        force: bool,
    ) -> SnowflakeCursor:
        # TODO: what to do with force?
        dbt_project_path = path.joinpath("dbt_project.yml")
        if not dbt_project_path.exists():
            raise ClickException(f"dbt_project.yml does not exist in provided path.")

        if dbt_version is None:
            with dbt_project_path.open() as fd:
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
            results = list(stage_manager.put_recursive(path, stage_name))
            cli_console.step(f"Copied {len(results)} files")

        with cli_console.phase("Creating DBT project"):
            staged_dbt_project_path = self._get_dbt_project_stage_path(stage_name)
            query = f"""CREATE OR REPLACE DBT PROJECT {name}
FROM {stage_name} MAIN_FILE='{staged_dbt_project_path}'
DBT_VERSION='{dbt_version}' DBT_ADAPTER_VERSION='{dbt_adapter_version}'"""
            return self.execute_query(query)

    def execute(self, dbt_command: str, name: str, *dbt_cli_args):
        query = f"EXECUTE DBT PROJECT {name} {dbt_command}"
        if dbt_cli_args:
            query += " " + " ".join([arg for arg in dbt_cli_args])
        return self.execute_query(query)

    @staticmethod
    def _get_dbt_project_stage_path(stage_name):
        return "/".join([stage_name, "dbt_project.yml"])
