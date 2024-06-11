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

import json

from snowflake.connector import SnowflakeConnection

from tests_integration.conftest import SnowCLIRunner
from tests_integration.test_utils import contains_row_with


class SnowparkJobsTestSetup:
    def __init__(
        self,
        runner: SnowCLIRunner,
        snowflake_session: SnowflakeConnection,
        test_root_path,
    ):
        self.runner = runner
        self.snowflake_session = snowflake_session
        self.test_root_path = test_root_path


class SnowparkJobsTestSteps:
    compute_pool = "snowcli_compute_pool"
    database = "snowcli_db"
    schema = "public"

    def __init__(self, setup: SnowparkJobsTestSetup):
        self._setup = setup

    def create_job(self) -> str:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "job",
                "create",
                "--compute-pool",
                self.compute_pool,
                "--spec-path",
                f"{self._setup.test_root_path}/spcs/spec/spec.yml",
                "--database",
                self.database,
                "--schema",
                self.schema,
            ],
        )
        assert isinstance(result.json, dict), result.output
        status = result.json["status"]
        assert status.__contains__("completed successfully")
        return status.replace("Job ", "").replace(
            " completed successfully with status: DONE.", ""
        )

    def status_should_return_job(self, job_id: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            ["spcs", "job", "status", job_id], connection="spcs"
        )
        assert isinstance(result.json, dict)
        status_json = result.json["SYSTEM$GET_JOB_STATUS"]
        status = json.loads(status_json)
        assert contains_row_with(
            status,
            {
                "serviceName": job_id,
                "status": "DONE",
                "message": "Completed successfully",
            },
        )

    def logs_should_return_job_logs(self, job_id: str) -> None:
        result = self._setup.runner.invoke_with_connection(
            ["spcs", "job", "logs", job_id, "--container-name", "hello-world"],
        )
        assert result.output
        assert result.output.strip() == f"{job_id}/0 Hello World!"

    def drop_job(self, job_id: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            ["spcs", "job", "drop", job_id],
        )
        assert result.json == {
            "SYSTEM$CANCEL_JOB": f"Job {job_id} successfully terminated"
        }
