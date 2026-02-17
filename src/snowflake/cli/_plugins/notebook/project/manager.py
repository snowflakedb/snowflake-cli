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

import logging
from typing import Optional

from snowflake.cli.api.sql_execution import SqlExecutionMixin

logger = logging.getLogger(__name__)


class NotebookProjectManager(SqlExecutionMixin):
    def list_projects(self):
        query = "SHOW NOTEBOOK PROJECTS"
        return self.execute_query(query)

    def create(self, name: str, source: str, comment: Optional[str]):
        query_parts = [
            f"CREATE NOTEBOOK PROJECT {name}",
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

    def _quote_string(self, value: str):
        if not (value.startswith("'") and value.endswith("'")):
            value = "'" + value.replace("'", "''") + "'"
        return value
