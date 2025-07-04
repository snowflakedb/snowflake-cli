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
from tests_integration.tests_using_container_services.spcs.testing_utils.compute_pool_utils import (
    ComputePoolTestSetup,
    ComputePoolTestSteps,
)


@pytest.mark.integration
def test_compute_pool(_test_steps: Tuple[ComputePoolTestSteps, str]):

    test_steps, compute_pool_name = _test_steps

    test_steps.create_compute_pool(compute_pool_name)
    test_steps.list_should_return_compute_pool(compute_pool_name)
    test_steps.stop_all_on_compute_pool(compute_pool_name)
    test_steps.suspend_compute_pool(compute_pool_name)
    test_steps.wait_until_compute_pool_is_suspended(compute_pool_name)
    test_steps.resume_compute_pool(compute_pool_name)
    test_steps.wait_until_compute_pool_is_idle(compute_pool_name)
    test_steps.set_unset_compute_pool_property(compute_pool_name)
    test_steps.drop_compute_pool(compute_pool_name)
    test_steps.list_should_not_return_compute_pool(compute_pool_name)


@pytest.mark.integration
def test_compute_pool_deploy_from_project_definition(
    _test_steps: Tuple[ComputePoolTestSteps, str],
    project_directory,
    alter_snowflake_yml,
):

    test_steps, compute_pool_name = _test_steps

    with project_directory("spcs_compute_pool"):
        alter_snowflake_yml(
            "snowflake.yml", "entities.compute_pool.identifier.name", compute_pool_name
        )

        test_steps.create_compute_pool_from_project_definition(compute_pool_name)
        test_steps.describe_should_return_compute_pool(compute_pool_name)

        alter_snowflake_yml(
            "snowflake.yml",
            "entities.compute_pool",
            {
                "type": "compute-pool",
                "identifier": {
                    "name": compute_pool_name,
                },
                "min_nodes": 1,
                "max_nodes": 2,
                "auto_resume": True,
                "auto_suspend_seconds": 10,
                "comment": "Upgraded compute pool",
                "tags": [
                    {"name": "new_tag", "value": "new_value"},
                ],
            },
        )
        test_steps.upgrade_compute_pool_from_project_definition()
        test_steps.describe_should_return_compute_pool(
            compute_pool_name, expected_values={"comment": "Upgraded compute pool"}
        )


@pytest.fixture
def _test_setup(runner, snowflake_session):
    compute_pool_test_setup = ComputePoolTestSetup(
        runner=runner, snowflake_session=snowflake_session
    )
    yield compute_pool_test_setup


@pytest.fixture
def _test_steps(_test_setup):
    compute_pool_name = f"compute_pool_{uuid.uuid4().hex}"
    test_steps = ComputePoolTestSteps(_test_setup)

    yield test_steps, compute_pool_name

    _test_setup.snowflake_session.execute_string(
        f"drop compute pool if exists {compute_pool_name}"
    )
