import time

import pytest
from snowflake.connector import SnowflakeConnection

from tests_integration.conftest import SnowCLIRunner
from tests_integration.test_utils import contains_row_with, not_contains_row_with


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

    def __init__(self, setup: SnowparkServicesTestSetup):
        self._setup = setup

    def create_service(self, service_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "containers",
                "service",
                "create",
                "--name",
                service_name,
                "--compute-pool",
                self.compute_pool,
                "--spec-path",
                f"{self._setup.test_root_path}/containers/spec/spec.yml",
                "--database",
                self.database,
                "--schema",
                self.schema,
            ],
            connection="spcs",
        )
        assert result.json == {
            "status": f"Service {service_name.upper()} successfully created."
        }

    def status_should_return_service(self, service_name: str) -> None:
        result = self._execute_status(service_name)
        assert contains_row_with(
            result.json,
            {"containerName": "hello-world", "serviceName": service_name.upper()},
        )

    def logs_should_return_service_logs(self, service_name: str) -> None:
        result = self._setup.runner.invoke_with_connection(
            [
                "containers",
                "service",
                "logs",
                service_name,
                "--container-name",
                "hello-world",
                "--instance-id",
                "0",
                "--database",
                self.database,
                "--schema",
                self.schema,
            ],
            connection="spcs",
        )
        assert result.output
        assert result.output.strip() == f"{service_name}/0 Hello World!"

    def list_should_return_service(self, service_name: str) -> None:
        result = self._execute_list()
        assert contains_row_with(result.json, {"name": service_name.upper()})

    def list_should_not_return_service(self, service_name: str) -> None:
        result = self._execute_list()
        assert not_contains_row_with(result.json, {"name": service_name.upper()})

    def describe_should_return_service(self, service_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "containers",
                "service",
                "desc",
                service_name,
                "--database",
                self.database,
                "--schema",
                self.schema,
            ],
            connection="spcs",
        )
        assert isinstance(result.json, dict)
        assert result.json["name"] == service_name.upper()

    def drop_service(self, service_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "containers",
                "service",
                "drop",
                service_name,
                "--database",
                self.database,
                "--schema",
                self.schema,
            ],
            connection="spcs",
        )
        assert result.json == {
            "status": f"{service_name.upper()} successfully dropped."
        }

    def wait_until_service_will_be_finish(self, service_name: str) -> None:
        wait_counter = 0
        max_counter = 90
        while wait_counter < max_counter:
            status = self._execute_status(service_name)
            if contains_row_with(
                status.json,
                {
                    "serviceName": service_name.upper(),
                    "status": "DONE",
                    "message": "Completed successfully",
                },
            ):
                return
            time.sleep(10)
            wait_counter += 1
        pytest.fail(f"{service_name} service didn't finish in 15 minutes")

    def _execute_status(self, service_name: str):
        return self._setup.runner.invoke_with_connection_json(
            [
                "containers",
                "service",
                "status",
                service_name,
                "--database",
                self.database,
                "--schema",
                self.schema,
            ],
            connection="spcs",
        )

    def _execute_list(self):
        return self._setup.runner.invoke_with_connection_json(
            [
                "containers",
                "service",
                "list",
                "--database",
                self.database,
                "--schema",
                self.schema,
            ],
            connection="spcs",
        )
