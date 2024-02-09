import json
import math
import time
from textwrap import dedent
from typing import Union

import pytest
from snowflake.connector import SnowflakeConnection

from tests_integration.conftest import SnowCLIRunner
from tests_integration.test_utils import contains_row_with, not_contains_row_with
from tests_integration.testing_utils.assertions.test_result_assertions import (
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
    database = "snowcli_db"
    schema = "public"
    container_name = "echo-test"

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
                self._get_spec_path(),
                "--database",
                self.database,
                "--schema",
                self.schema,
            ],
        )
        assert_that_result_is_successful_and_output_json_equals(
            result, {"status": f"Service {service_name.upper()} successfully created."}
        )

    def status_should_return_service(self, service_name: str) -> None:
        result = self._execute_status(service_name)
        assert_that_result_is_successful_and_output_json_contains(
            result,
            {"containerName": self.container_name, "serviceName": service_name.upper()},
        )

    def logs_should_return_service_logs(self, service_name: str) -> None:
        result = self._setup.runner.invoke_with_connection(
            [
                "spcs",
                "service",
                "logs",
                service_name,
                "--container-name",
                self.container_name,
                "--instance-id",
                "0",
                "--database",
                self.database,
                "--schema",
                self.schema,
            ],
        )
        assert result.output
        # Assert this instead of full payload due to log coloring
        assert service_name in result.output
        assert '"GET /healthcheck HTTP/1.1" 200 -' in result.output

    def list_should_return_service(self, service_name: str) -> None:
        result = self._execute_list()
        assert contains_row_with(result.json, {"name": service_name.upper()})

    def list_should_not_return_service(self, service_name: str) -> None:
        result = self._execute_list()
        assert not_contains_row_with(result.json, {"name": service_name.upper()})

    def describe_should_return_service(self, service_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "describe",
                "service",
                f"{self.database}.{self.schema}.{service_name}",
            ],
        )
        assert result.json
        assert result.json[0]["name"] == service_name.upper()  # type: ignore

    def drop_service(self, service_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "drop",
                "service",
                f"{self.database}.{self.schema}.{service_name}",
            ],
        )
        assert result.json[0] == {  # type: ignore
            "status": f"{service_name.upper()} successfully dropped."
        }

    def wait_until_service_is_ready(self, service_name: str) -> None:
        self._wait_until_service_reaches_state(service_name, "READY", 900)

    def _wait_until_service_reaches_state(
        self, service_name: str, target_status: Union[str, dict], max_duration: int
    ):
        assert max_duration > 0
        max_counter = math.ceil(max_duration / 10)
        if isinstance(target_status, str):
            target_status = {"status": target_status}
        for i in range(max_counter):
            status = self._execute_status(service_name)
            if contains_row_with(status.json, target_status):
                return
            elif contains_row_with(status.json, {"status": "FAILED"}):
                describe = self._setup.runner.invoke_with_connection_json(
                    ["object", "describe", "service", service_name]
                )
                pytest.fail(
                    dedent(
                        f"""
                    {service_name} service failed before reaching target state:
                    {json.dumps(target_status)}
                    current state:
                    {json.dumps(status)}
                    current describe:
                    {json.dumps(describe)}
                    """
                    )
                )
            time.sleep(10)
        status = self._execute_status(service_name)

        error_message = f"""
{service_name} service didn't reach target state in {max_duration} seconds.
target:
{json.dumps(target_status)}
current:
{json.dumps(status.json)}
"""
        pytest.fail(error_message)

    def _execute_status(self, service_name: str):
        return self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "service",
                "status",
                service_name,
                "--database",
                self.database,
                "--schema",
                self.schema,
            ],
        )

    def _execute_list(self):
        return self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "list",
                "service",
            ],
        )

    def _get_spec_path(self):
        return f"{self._setup.test_root_path}/spcs/spec/spec.yml"
