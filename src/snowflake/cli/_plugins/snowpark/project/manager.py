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

from click import ClickException
from snowflake.cli.api.sql_execution import SqlExecutionMixin

log = logging.getLogger(__name__)


class SnowflakeProjectManager(SqlExecutionMixin):
    def _set_session_config(self):
        session_config = ["ALTER SESSION SET ENABLE_SNOWPARK_PROJECT = 'ENABLE'"]
        try:
            for config in session_config:
                self.execute_query(config)
        except Exception as e:
            raise ClickException(f"Failed to set session config: {e}")

    def create(self, name: str, stage: str, overwrite: bool):
        query_parts = ["CREATE"]
        if overwrite:
            query_parts.append("OR REPLACE")
        query_parts.append(f"SNOWPARK PROJECT {name} FROM {stage}")
        try:
            self._set_session_config()
            result = self.execute_query(" ".join(query_parts)).fetchone()
            log.debug("Snowpark project '%s' created successfully.", name)
            log.debug("Result: %s", result)
            return result[0]
        except Exception as e:
            raise ClickException(f"Failed to create Snowpark project: {e}")

    def drop(self, name: str):
        query = f"DROP SNOWPARK PROJECT {name}"
        try:
            self._set_session_config()
            result = self.execute_query(query).fetchone()
            log.debug("Snowpark project '%s' dropped successfully.", name)
            log.debug("Result: %s", result)
            return result[0]
        except Exception as e:
            raise ClickException(f"Failed to drop Snowpark project: {e}")

    def list_projects(self):
        query = "SHOW SNOWPARK PROJECTS"
        try:
            self._set_session_config()
            return self.execute_query(query)
        except Exception as e:
            raise ClickException(f"Failed to list Snowpark projects: {e}")

    def execute(self, name: str, entrypoint: str):
        query_parts = ["EXECUTE SNOWPARK PROJECT", name, f"ENTRYPOINT='{entrypoint}'"]
        try:
            self._set_session_config()
            return self.execute_query(" ".join(query_parts))
        except Exception as e:
            raise ClickException(f"Failed to execute Snowpark project: {e}")
