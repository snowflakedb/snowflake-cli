# Copyright (c) 2026 Snowflake Inc.
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

import glob
import logging
import os
import random
from pathlib import Path
from typing import List, Optional

from click import ClickException
from snowflake.cli.api.sql_execution import SqlExecutionMixin

logger = logging.getLogger(__name__)


class NotebookProjectManager(SqlExecutionMixin):
    def list_projects(self):
        query = "SHOW NOTEBOOK PROJECTS"
        return self.execute_query(query)

    def create(
        self,
        name: str,
        source: str,
        comment: Optional[str] = None,
        overwrite: bool = False,
        skip_if_exists: bool = False,
    ):
        create_project = "CREATE NOTEBOOK PROJECT"
        if overwrite and skip_if_exists:
            raise ValueError("overwrite and skip_if_exists cannot be used together")
        if overwrite:
            create_project = "CREATE OR REPLACE NOTEBOOK PROJECT"
        if skip_if_exists:
            create_project = "CREATE NOTEBOOK PROJECT IF NOT EXISTS"
        query_parts = [
            create_project,
            name,
            f"FROM {self._quote_string(source)}",
        ]
        if comment:
            query_parts.append(f"COMMENT = {self._quote_string(comment)}")
        query = " ".join(query_parts)
        result = self.execute_query(query).fetchone()
        logger.info(
            "Created notebook project %s from %s with comment %s", name, source, comment
        )
        logger.debug("Result: %s", result)
        return result[0]

    def drop(self, name: str):
        query = f"DROP NOTEBOOK PROJECT {name}"
        result = self.execute_query(query).fetchone()
        logger.info("Drop notebook project %s", name)
        logger.debug("Result: %s", result)
        return result[0]

    def execute(
        self,
        name: str,
        arguments: Optional[List[str]],
        main_file: str,
        compute_pool: Optional[str],
        query_warehouse: Optional[str],
        runtime: Optional[str],
        requirements_file: Optional[str],
        external_access_integrations: Optional[List[str]],
    ):
        query_parts = [
            f"EXECUTE NOTEBOOK PROJECT {name}",
            f"MAIN_FILE = {self._quote_string(main_file)}",
        ]
        if compute_pool:
            query_parts.append(f"COMPUTE_POOL = {self._quote_string(compute_pool)}")
        if query_warehouse:
            query_parts.append(
                f"QUERY_WAREHOUSE = {self._quote_string(query_warehouse)}"
            )
        if runtime:
            query_parts.append(f"RUNTIME = {self._quote_string(runtime)}")
        if requirements_file:
            query_parts.append(
                f"REQUIREMENTS_FILE = {self._quote_string(requirements_file)}"
            )
        if external_access_integrations:
            eais = []
            for integration in external_access_integrations:
                eais.append(self._quote_string(integration))
            query_parts.append(f"EXTERNAL_ACCESS_INTEGRATIONS = ({','.join(eais)})")
        if arguments:
            query_parts.append(f"ARGUMENTS = {self._quote_string(' '.join(arguments))}")
        result = self.execute_query(" ".join(query_parts)).fetchone()
        logger.info("Executed notebook project %s", name)
        logger.debug("Result: %s", result)
        return result[0]

    def _quote_string(self, value: str):
        if not (value.startswith("'") and value.endswith("'")):
            value = "'" + value.replace("'", "''") + "'"
        return value

    def process_source(self, source: str) -> str:
        if source.startswith("@"):
            return source
        source_lower = source.lower()
        if source_lower.startswith("snow://"):
            return source
        if source_lower.startswith("file://") or "://" not in source:
            local_path = source[7:] if source_lower.startswith("file://") else source
            stage_name = f"tmp_npo_stage_{random.randint(1000000, 9999999)}"
            self.execute_query(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")
            self._upload_directory_recursive(local_path, stage_name)
            return f"@{stage_name}"
        raise ValueError(
            f"Invalid source: '{source}'. Source must be a Snowflake stage path (starting with '@'), a Snowflake workspace path (starting with 'snow://'), or a local file system path (starting with 'file://' or no protocol prefix)."
        )

    def _upload_directory_recursive(self, local_path: str, stage_name: str) -> None:
        """Upload all files from local_path to stage, preserving directory structure."""
        root_path = Path(local_path)
        if not root_path.is_dir():
            raise ClickException(f"Source path '{local_path}' is not a directory.")
        glob_pattern = os.path.join(glob.escape(local_path), "**", "*")

        for file_path_str in glob.iglob(glob_pattern, recursive=True):
            file_path = Path(file_path_str)
            if file_path.is_file():
                relative_path = file_path.relative_to(root_path)
                stage_subdir = str(relative_path.parent)
                if stage_subdir == ".":
                    stage_dest = f"@{stage_name}"
                else:
                    stage_dest = f"@{stage_name}/{stage_subdir}"
                self.execute_query(
                    f"PUT file://{file_path} {stage_dest} auto_compress=false"
                )
