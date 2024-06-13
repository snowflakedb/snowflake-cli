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

from pathlib import Path

from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class JobManager(SqlExecutionMixin):
    def create(self, compute_pool: str, spec_path: Path) -> SnowflakeCursor:
        spec = self._read_yaml(spec_path)
        return self._execute_schema_query(
            f"""\
            EXECUTE SERVICE
            IN COMPUTE POOL {compute_pool}
            FROM SPECIFICATION $$
            {spec}
            $$
            """
        )

    def _read_yaml(self, path: Path) -> str:
        # TODO(aivanou): Add validation towards schema
        # TODO(aivanou): Combine this with service manager
        import json

        import yaml

        with SecurePath(path).open("r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fh:
            data = yaml.safe_load(fh)
        return json.dumps(data)

    def status(self, job_name: str) -> SnowflakeCursor:
        return self._execute_query(f"CALL SYSTEM$GET_JOB_STATUS('{job_name}')")

    def logs(self, job_name: str, container_name: str):
        return self._execute_query(
            f"call SYSTEM$GET_JOB_LOGS('{job_name}', '{container_name}')"
        )
