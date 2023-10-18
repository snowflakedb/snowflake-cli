from __future__ import annotations

import pytest

from tests_integration.snowflake_connector import test_database, snowflake_session
from tests_integration.testing_utils.snowpark_utils import (
    TestType,
    SnowparkTestSetup,
    SnowparkTestSteps,
)
from tests_integration.testing_utils.sql_utils import sql_test_helper
from tests_integration.testing_utils.naming_utils import object_name_provider
from tests_integration.testing_utils.working_directory_utils import (
    temporary_working_directory,
)
from tests_integration.conftest import SnowCLIRunner


@pytest.mark.integration
def test_snowpark_function_flow(_test_steps, project_file):
    _test_steps.assert_that_no_entities_are_in_snowflake()
    _test_steps.assert_that_no_files_are_staged_in_test_db()

    _test_steps.snowpark_list_should_return_no_data()

    _test_steps.snowpark_init_should_initialize_files_with_default_content()
    _test_steps.snowpark_package_should_zip_files()

    function_name = _test_steps.get_entity_name()

    with project_file(
        "snowpark_functions",
        merge_project_definition={
            "functions": [
                {
                    "name": function_name,
                    "handler": "app.hello",
                    "signature": [{"name": "name", "type": "string"}],
                    "returns": "string",
                }
            ]
        },
    ):
        _test_steps.run_deploy()

    _test_steps.assert_that_only_these_entities_are_in_snowflake(
        f"{function_name}(VARCHAR) RETURN VARCHAR"
    )
    _test_steps.assert_that_only_these_files_are_staged_in_test_db(
        f"deployments/{function_name}_name_string/app.zip"
    )

    _test_steps.snowpark_list_should_return_entity_at_first_place(
        entity_name=function_name,
        arguments="(VARCHAR)",
        result_type="VARCHAR",
    )

    _test_steps.snowpark_describe_should_return_entity_description(
        entity_name=function_name, arguments="(VARCHAR)", signature="(NAME VARCHAR)"
    )

    _test_steps.snowpark_execute_should_return_expected_value(
        entity_name=function_name,
        arguments="('foo')",
        expected_value="Hello foo!",
    )

    _test_steps.assert_that_only_these_files_are_staged_in_test_db(
        f"deployments/{function_name}_name_string/app.zip"
    )

    with project_file(
        "snowpark_functions",
        merge_project_definition={
            "functions": [
                {
                    "name": function_name,
                    "handler": "app.hello",
                    "signature": [{"name": "name", "type": "string"}],
                    "returns": "variant",
                }
            ]
        },
    ):
        _test_steps.run_deploy("--replace")

    _test_steps.snowpark_describe_should_return_entity_description(
        entity_name=function_name,
        arguments="(VARCHAR)",
        signature="(NAME VARCHAR)",
        returns="VARIANT",
    )

    _test_steps.snowpark_drop_should_finish_successfully(
        entity_name=function_name,
        arguments="(VARCHAR)",
    )
    _test_steps.assert_that_no_entities_are_in_snowflake()
    _test_steps.assert_that_only_these_files_are_staged_in_test_db(
        f"deployments/{function_name}_name_string/app.zip"
    )

    _test_steps.snowpark_list_should_return_no_data()


@pytest.fixture
def _test_setup(
    runner,
    snowflake_session,
    test_database,
    sql_test_helper,
    object_name_provider,
    temporary_working_directory,
    snapshot,
):
    snowpark_function_test_setup = SnowparkTestSetup(
        runner=runner,
        snowflake_session=snowflake_session,
        sql_test_helper=sql_test_helper,
        object_name_provider=object_name_provider,
        snapshot=snapshot,
        test_database=test_database,
        test_type=TestType.FUNCTION,
    )
    yield snowpark_function_test_setup
    snowpark_function_test_setup.clean_after_test_case()


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkTestSteps(_test_setup, TestType.FUNCTION)
