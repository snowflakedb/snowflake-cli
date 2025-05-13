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
import uuid
from typing import Tuple

import pytest

from tests_integration.spcs.testing_utils.spcs_services_utils import (
    SnowparkServicesTestSetup,
    SnowparkServicesTestSteps,
)


@pytest.mark.integration
@pytest.mark.skip("Skipped temporarily")
def test_services(_test_steps: Tuple[SnowparkServicesTestSteps, str]):

    test_steps, service_name = _test_steps

    # test long-running service
    test_steps.create_service(service_name)
    test_steps.create_second_service(service_name)
    test_steps.list_instances_should_show_instances(service_name)
    test_steps.list_containers_should_show_containers(service_name)
    test_steps.list_roles_should_show_roles(service_name)
    test_steps.status_should_return_service(service_name, "hello-world")
    test_steps.list_should_return_service(service_name)
    test_steps.wait_until_service_is_running(service_name)
    test_steps.logs_should_return_service_logs(
        service_name, "hello-world", "Serving Flask app 'echo_service'"
    )
    test_steps.suspend_service(service_name)
    test_steps.wait_until_service_is_suspended(service_name)
    test_steps.resume_service(service_name)
    test_steps.wait_until_service_is_running(service_name)
    test_steps.describe_should_return_service(service_name)
    test_steps.list_endpoints_should_show_endpoint(service_name)
    test_steps.list_instances_should_show_instances(service_name)
    test_steps.list_containers_should_show_containers(service_name)
    test_steps.list_roles_should_show_roles(service_name)
    test_steps.upgrade_service_should_change_spec(service_name)
    test_steps.metrics_should_include_services_from_both_dbs(
        service_name, "hello-world"
    )
    test_steps.metrics_with_fqn_should_include_only_one_service(
        service_name, test_steps.database, "hello-world"
    )
    test_steps.metrics_with_fqn_should_include_only_one_service(
        service_name, test_steps.another_database, "hello-world"
    )
    test_steps.set_unset_service_property(service_name)
    test_steps.drop_service(service_name)
    test_steps.list_should_not_return_service(service_name)


@pytest.mark.integration
def test_service_create_from_project_definition(
    _test_steps: Tuple[SnowparkServicesTestSteps, str],
    alter_snowflake_yml,
    project_directory,
):
    test_steps, service_name = _test_steps
    stage = f"{service_name}_stage"

    with project_directory("spcs_service"):
        alter_snowflake_yml("snowflake.yml", "entities.service.stage", stage)
        alter_snowflake_yml(
            "snowflake.yml", "entities.service.identifier.name", service_name
        )

        test_steps.deploy_service(service_name)
        test_steps.describe_should_return_service(service_name)

        alter_snowflake_yml(
            "snowflake.yml",
            "entities.service",
            {
                "type": "service",
                "identifier": {
                    "name": service_name,
                },
                "stage": f"{stage}_upgrade",
                "compute_pool": "snowcli_compute_pool",
                "spec_file": "spec_upgrade.yml",
                "min_instances": 1,
                "max_instances": 2,
                "query_warehouse": "xsmall",
                "comment": "Upgraded service",
                "artifacts": ["spec_upgrade.yml"],
            },
        )
        test_steps.upgrade_service()
        test_steps.describe_should_return_service(
            service_name,
            expected_values_contain={
                "comment": "Upgraded service",
                "spec": 'UPGRADED: "true"',
            },
        )


@pytest.mark.integration
@pytest.mark.xfail(reason="Consistently timing out on execute call")
def test_job_services(_test_steps: Tuple[SnowparkServicesTestSteps, str]):

    test_steps, job_service_name = _test_steps

    # test job service
    test_steps.execute_job_service(job_service_name)
    test_steps.status_should_return_service(job_service_name, "main")
    test_steps.describe_should_return_service(job_service_name)
    test_steps.list_should_return_service(job_service_name)
    test_steps.logs_should_return_service_logs(job_service_name, "main", "processing 0")
    test_steps.drop_service(job_service_name)
    test_steps.list_should_not_return_service(job_service_name)


@pytest.fixture
def _test_setup(
    runner,
    snowflake_session,
    test_root_path,
):
    snowpark_function_test_setup = SnowparkServicesTestSetup(
        runner=runner,
        snowflake_session=snowflake_session,
        test_root_path=test_root_path,
    )
    yield snowpark_function_test_setup


@pytest.fixture
def _test_steps(_test_setup):
    random_uuid = uuid.uuid4().hex
    service_name = f"spcs_service_{random_uuid}"
    test_steps = SnowparkServicesTestSteps(_test_setup)

    yield test_steps, service_name

    service_fqn = f"{test_steps.database}.{test_steps.schema}.{service_name}"
    _test_setup.snowflake_session.execute_string(
        f"drop service if exists {service_fqn}"
    )
