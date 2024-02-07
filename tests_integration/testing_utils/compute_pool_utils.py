import math
import time

import pytest
from snowflake.connector import SnowflakeConnection

from tests_integration.conftest import SnowCLIRunner
from tests_integration.test_utils import contains_row_with, not_contains_row_with
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful_and_executed_successfully,
)


class ComputePoolTestSetup:
    def __init__(
        self,
        runner: SnowCLIRunner,
        snowflake_session: SnowflakeConnection,
    ):
        self.runner = runner
        self.snowflake_session = snowflake_session


class ComputePoolTestSteps:
    def __init__(self, setup: ComputePoolTestSetup):
        self._setup = setup

    def create_compute_pool(self, compute_pool_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "compute-pool",
                "create",
                compute_pool_name,
                "--min-nodes",
                1,
                "--family",
                "CPU_X64_XS",
            ]
        )
        assert result.json, result.output
        assert "status" in result.json
        assert (
            f"Compute Pool {compute_pool_name.upper()} successfully created."
            in result.json["status"]  # type: ignore
        )

    def list_should_return_compute_pool(self, compute_pool_name) -> None:
        result = self._execute_list()
        assert contains_row_with(result.json, {"name": compute_pool_name.upper()})

    def list_should_not_return_compute_pool(self, compute_pool_name: str) -> None:
        result = self._execute_list()
        assert not_contains_row_with(result.json, {"name": compute_pool_name.upper()})

    def describe_should_return_compute_pool(self, compute_pool_name: str) -> None:
        result = self._execute_describe(compute_pool_name)
        assert result.json
        assert len(result.json) == 1
        assert result.json[0]["name"] == compute_pool_name.upper()

    def drop_compute_pool(self, compute_pool_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "drop",
                "compute-pool",
                compute_pool_name,
            ],
        )
        assert result.json
        assert len(result.json) == 1
        assert result.json[0] == {  # type: ignore
            "status": f"{compute_pool_name.upper()} successfully dropped."
        }

    def stop_all_on_compute_pool(self, compute_pool_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            ["spcs", "compute-pool", "stop-all", compute_pool_name]
        )
        assert_that_result_is_successful_and_executed_successfully(result, is_json=True)

    def suspend_compute_pool(self, compute_pool_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            ["spcs", "compute-pool", "suspend", compute_pool_name]
        )
        assert_that_result_is_successful_and_executed_successfully(result, is_json=True)

    def resume_compute_pool(self, compute_pool_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            ["spcs", "compute-pool", "resume", compute_pool_name]
        )
        assert_that_result_is_successful_and_executed_successfully(result, is_json=True)

    def wait_until_compute_pool_is_idle(self, compute_pool_name: str) -> None:
        self._wait_until_compute_pool_reaches_state(compute_pool_name, "IDLE", 300)

    def wait_until_compute_pool_is_suspended(self, compute_pool_name: str) -> None:
        self._wait_until_compute_pool_reaches_state(compute_pool_name, "SUSPENDED", 60)

    def _wait_until_compute_pool_reaches_state(
        self, compute_pool_name: str, target_state: str, max_duration: int
    ):
        assert max_duration > 0
        max_counter = math.ceil(max_duration / 10)
        target_state = target_state.upper()
        for i in range(max_counter):
            status = self._execute_describe(compute_pool_name)
            if contains_row_with(status.json, {"state": target_state}):
                return
            time.sleep(10)
        status = self._execute_describe(compute_pool_name)

        error_message = f"Compute pool {compute_pool_name} didn't reach target state '{target_state}' in {max_duration} seconds. Current state is '{status.json['state']}'"
        pytest.fail(error_message)

    def _execute_describe(self, compute_pool_name: str):
        return self._setup.runner.invoke_with_connection_json(
            ["object", "describe", "compute-pool", compute_pool_name]
        )

    def _execute_list(self):
        return self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "list",
                "compute-pool",
            ],
        )
