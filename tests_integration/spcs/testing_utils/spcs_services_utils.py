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
import math
import os
import time
from textwrap import dedent

import pytest
from snowflake.connector import SnowflakeConnection

from tests_integration.conftest import SnowCLIRunner
from tests_integration.test_utils import contains_row_with, not_contains_row_with
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful_and_executed_successfully,
    assert_that_result_is_successful_and_output_json_contains,
    assert_that_result_is_successful_and_output_json_equals,
)


class SnowparkServicesTestSetup:
    def __init__(
        self,
        runner: SnowCLIRunner,
        snowflake_session: SnowflakeConnection,
        test_root_path,
    ):
        self.runner = runner
        self.snowflake_session = snowflake_session
        self.test_root_path = test_root_path


class SnowparkServicesTestSteps:
    compute_pool = "snowcli_compute_pool"
    database = os.environ.get(
        "SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE", "SNOWCLI_DB"
    )
    schema = "public"
    container_name = "hello-world"

    def __init__(self, setup: SnowparkServicesTestSetup):
        self._setup = setup

    def create_service(self, service_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "service",
                "create",
                service_name,
                "--compute-pool",
                self.compute_pool,
                "--spec-path",
                self._get_spec_path("spec.yml"),
                *self._database_schema_args(),
            ],
        )
        assert_that_result_is_successful_and_output_json_equals(
            result, {"status": f"Service {service_name.upper()} successfully created."}
        )

    def execute_job_service(self, job_service_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "service",
                "execute-job",
                job_service_name,
                "--compute-pool",
                self.compute_pool,
                "--spec-path",
                self._get_spec_path("job_service_spec.yaml"),
                *self._database_schema_args(),
            ],
        )
        assert_that_result_is_successful_and_output_json_equals(
            result,
            {
                "status": f"Job {job_service_name.upper()} completed successfully with status: DONE."
            },
        )

    def status_should_return_service(
        self, service_name: str, container_name: str
    ) -> None:
        result = self._execute_status(service_name)
        assert_that_result_is_successful_and_output_json_contains(
            result,
            {"containerName": container_name, "serviceName": service_name.upper()},
        )

    def logs_should_return_service_logs(
        self, service_name: str, container_name: str, expected_log: str
    ) -> None:
        result = self._execute_logs(service_name, container_name)
        assert result.output
        # Assert this instead of full payload due to log coloring
        assert service_name in result.output
        assert expected_log in result.output

    def list_should_return_service(self, service_name: str) -> None:
        result = self._execute_list()
        assert contains_row_with(result.json, {"name": service_name.upper()})

    def list_should_not_return_service(self, service_name: str) -> None:
        result = self._execute_list()
        assert not_contains_row_with(result.json, {"name": service_name.upper()})

    def describe_should_return_service(self, service_name: str) -> None:
        result = self._execute_describe(service_name)
        assert result.json
        assert result.json[0]["name"] == service_name.upper()  # type: ignore

    def set_unset_service_property(self, service_name: str) -> None:
        comment = "test comment"
        set_result = self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "service",
                "set",
                service_name,
                "--comment",
                comment,
                *self._database_schema_args(),
            ]
        )
        assert_that_result_is_successful_and_executed_successfully(
            set_result, is_json=True
        )

        description = self._execute_describe(service_name)
        assert contains_row_with(description.json, {"comment": comment})
        unset_result = self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "service",
                "unset",
                service_name,
                "--comment",
                *self._database_schema_args(),
            ]
        )
        assert_that_result_is_successful_and_executed_successfully(
            unset_result, is_json=True
        )
        description = self._execute_describe(service_name)
        assert contains_row_with(description.json, {"comment": None})

    def drop_service(self, service_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "drop",
                "service",
                self._get_fqn(service_name),
            ],
        )
        assert result.json[0] == {  # type: ignore
            "status": f"{service_name.upper()} successfully dropped."
        }

    def wait_until_service_is_running(self, service_name: str) -> None:
        self._wait_until_service_reaches_state(service_name, "RUNNING", 900)

    def suspend_service(self, service_name: str):
        result = self._setup.runner.invoke_with_connection_json(
            ["spcs", "service", "suspend", service_name, *self._database_schema_args()]
        )
        assert_that_result_is_successful_and_executed_successfully(result, is_json=True)

    def wait_until_service_is_suspended(self, service_name: str) -> None:
        self._wait_until_service_reaches_state(service_name, "SUSPENDED", 60)

    def resume_service(self, service_name: str):
        result = self._setup.runner.invoke_with_connection_json(
            ["spcs", "service", "resume", service_name, *self._database_schema_args()]
        )
        assert_that_result_is_successful_and_executed_successfully(result, is_json=True)

    def _wait_until_service_reaches_state(
        self, service_name: str, target_status: str, max_duration: int
    ):
        assert max_duration > 0
        max_counter = math.ceil(max_duration / 10)
        for i in range(max_counter):
            desc_res = self._execute_describe(service_name)
            if desc_res.json[0]["status"] == target_status:
                return
            time.sleep(10)
        containers_status = self._execute_status(service_name)
        error_message = dedent(
            f"""
            {service_name} service didn't reach target state {target_status} in {max_duration} seconds.
            service status:
            {desc_res.json}
            containers status:
            {json.dumps(containers_status.json)}
            """
        ).strip()
        pytest.fail(error_message)

    def upgrade_service_should_change_spec(self, service_name: str):
        new_container_name = "goodbye-world"

        describe_result = self._execute_describe(service_name)
        assert describe_result.exit_code == 0, describe_result.output
        assert (
            new_container_name not in describe_result.json[0]["spec"]
        ), f"Container name '{new_container_name}' found in output of DESCRIBE SERVICE before spec has been updated. This is unexpected."

        spec_path = f"{self._setup.test_root_path}/spcs/spec/spec_upgrade.yml"
        upgrade_result = self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "service",
                "upgrade",
                service_name,
                "--spec-path",
                spec_path,
                *self._database_schema_args(),
            ]
        )
        assert_that_result_is_successful_and_executed_successfully(
            upgrade_result, is_json=True
        )

        describe_result = self._execute_describe(service_name)
        assert describe_result.exit_code == 0, describe_result.output
        # do not assert direct equality because the spec field in output of DESCRIBE SERVICE has some extra info
        assert (
            new_container_name in describe_result.json[0]["spec"]
        ), f"Container name '{new_container_name}' from spec_upgrade.yml not found in output of DESCRIBE SERVICE."

    def list_endpoints_should_show_endpoint(self, service_name: str):
        result = self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "service",
                "list-endpoints",
                service_name,
                *self._database_schema_args(),
            ]
        )
        assert_that_result_is_successful_and_output_json_contains(
            result,
            {
                "name": "echoendpoint",
            },
        )

    def _execute_status(self, service_name: str):
        return self._setup.runner.invoke_with_connection_json(
            ["spcs", "service", "status", service_name, *self._database_schema_args()],
        )

    def _execute_list(self):
        return self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "list",
                "service",
            ],
        )

    def _execute_describe(self, service_name: str):
        return self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "describe",
                "service",
                f"{self.database}.{self.schema}.{service_name}",
            ],
        )

    def _execute_logs(
        self, service_name: str, container_name: str, num_lines: int = 500
    ):
        return self._setup.runner.invoke_with_connection(
            [
                "spcs",
                "service",
                "logs",
                service_name,
                "--container-name",
                container_name,
                "--instance-id",
                "0",
                "--num-lines",
                str(num_lines),
                *self._database_schema_args(),
            ],
        )

    def _get_spec_path(self, spec_file_name) -> str:
        return self._setup.test_root_path / "spcs" / "spec" / spec_file_name

    def _get_fqn(self, service_name) -> str:
        return f"{self.database}.{self.schema}.{service_name}"

    def _database_schema_args(self):
        return (
            "--database",
            self.database,
            "--schema",
            self.schema,
        )
