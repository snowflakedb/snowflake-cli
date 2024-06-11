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

import pytest

from tests.testing_utils.fixtures import alter_snowflake_yml
from tests_integration.testing_utils import SnowparkTestSetup, SnowparkTestSteps

STAGE_NAME = "dev_deployment"


@pytest.mark.integration
def test_snowpark_external_access(project_directory, _test_steps, test_database):

    with project_directory("snowpark_external_access") as project_dir:

        _test_steps.snowpark_build_should_zip_files()

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{test_database.upper()}.PUBLIC.status_procedure()",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{test_database.upper()}.PUBLIC.status_function()",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier=f"status_function()",
            expected_value="200",
        )
        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier=f"status_procedure()",
            expected_value="200",
        )


@pytest.mark.integration
def test_snowpark_upgrades_with_external_access(
    project_directory, _test_steps, test_database, alter_snowflake_yml
):

    with project_directory("snowpark") as tmp_dir:
        _test_steps.snowpark_build_should_zip_files()

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{test_database.upper()}.PUBLIC.hello_procedure(name string)",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{test_database.upper()}.PUBLIC.test()",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{test_database.upper()}.PUBLIC.hello_function(name string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.0.external_access_integrations",
            value=["snowflake_docs_access_integration"],
        )
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.procedures.0.external_access_integrations",
            value=["snowflake_docs_access_integration"],
        )

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{test_database.upper()}.PUBLIC.hello_procedure(name string)",
                    "status": "definition updated",
                    "type": "procedure",
                },
                {
                    "object": f"{test_database.upper()}.PUBLIC.test()",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": f"{test_database.upper()}.PUBLIC.hello_function(name string)",
                    "status": "definition updated",
                    "type": "function",
                },
            ],
            additional_arguments=["--replace"],
        )

        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.0.external_access_integrations",
            value=["CLI_TEST_INTEGRATION"],
        )

        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.procedures.0.external_access_integrations",
            value=["CLI_TEST_INTEGRATION"],
        )

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{test_database.upper()}.PUBLIC.hello_procedure(name string)",
                    "status": "definition updated",
                    "type": "procedure",
                },
                {
                    "object": f"{test_database.upper()}.PUBLIC.test()",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": f"{test_database.upper()}.PUBLIC.hello_function(name string)",
                    "status": "definition updated",
                    "type": "function",
                },
            ],
            additional_arguments=["--replace"],
        )

        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.0.external_access_integrations",
            value=[],
        )
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.procedures.0.external_access_integrations",
            value=[],
        )
        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{test_database.upper()}.PUBLIC.hello_procedure(name string)",
                    "status": "definition updated",
                    "type": "procedure",
                },
                {
                    "object": f"{test_database.upper()}.PUBLIC.test()",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": f"{test_database.upper()}.PUBLIC.hello_function(name string)",
                    "status": "definition updated",
                    "type": "function",
                },
            ],
            additional_arguments=["--replace"],
        )


@pytest.fixture
def _test_setup(
    runner,
    sql_test_helper,
    test_database,
):
    snowpark_test_setup = SnowparkTestSetup(
        runner=runner,
        sql_test_helper=sql_test_helper,
        test_database=test_database,
        snapshot=None,  # not needed
    )
    yield snowpark_test_setup


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkTestSteps(_test_setup)
