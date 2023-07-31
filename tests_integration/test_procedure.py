from __future__ import annotations

import re
from typing import Any, List, Dict

import pytest
from snowflake.connector import SnowflakeConnection
from syrupy import SnapshotAssertion

from tests_integration.testing_utils.file_utils import replace_text_in_file
from tests_integration.conftest import runner
from tests_integration.snowflake_connector import create_database, snowflake_session
from tests_integration.testing_utils.snowpark_utils import SnowparkTestSteps, SnowparkTestSetup
from tests_integration.testing_utils.sql_utils import sql_test_helper
from tests_integration.testing_utils.naming_utils import object_name_provider
from tests_integration.testing_utils.working_directory_utils import (
    temporary_working_directory,
)

from tests_integration.conftest import SnowCLIRunner
from tests_integration.testing_utils.assertions.test_file_assertions import (
    assert_that_current_working_directory_contains_only_following_files,
    assert_that_file_content_is_equal_to_snapshot,
)
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful_and_has_no_output,
    assert_that_result_is_successful_and_output_contains,
    assert_that_result_is_successful,
    assert_that_result_is_successful_and_json_output_contains_value_at_path,
)
from tests_integration.testing_utils.naming_utils import ObjectNameProvider
from tests_integration.testing_utils.sql_utils import SqlTestHelper


@pytest.mark.integration
def test_snowpark_procedure_flow(_test_steps):
    _test_steps.assert_that_no_procedures_are_in_snowflake()
    _test_steps.assert_that_no_files_are_staged_in_snowflake()

    _test_steps.snowpark_procedure_list_should_return_no_data()

    _test_steps.snowpark_procedure_init_should_initialize_files_with_default_content()
    _test_steps.snowpark_procedure_package_should_zip_files()

    procedure_name = _test_steps.snowpark_procedure_create_should_finish_successfully()
    _test_steps.assert_that_only_these_procedures_are_in_snowflake(
        f"{procedure_name}() RETURN VARCHAR"
    )
    _test_steps.assert_that_only_these_files_are_staged_in_snowflake(
        f"deployments/{procedure_name}/app.zip"
    )

    _test_steps.snowpark_procedures_list_should_return_list_containing_procedure_at_the_first_place(
        procedure_name=procedure_name,
        arguments="()",
        result_type="VARCHAR",
    )

    _test_steps.snowpark_procedure_describe_should_return_procedure_description(
        procedure_name=procedure_name,
        arguments="()",
    )

    _test_steps.snowpark_procedure_execute_should_return_expected_value(
        procedure_name=procedure_name,
        arguments="()",
        expected_value="Hello World!",
    )

    _test_steps.snowpark_procedure_update_should_finish_successfully(procedure_name)
    _test_steps.assert_that_only_these_procedure_are_in_snowflake(
        f"{procedure_name}() RETURN NUMBER"
    )
    _test_steps.assert_that_only_these_files_are_staged_in_snowflake(
        f"deployments/{procedure_name}/app.zip"
    )

    _test_steps.snowpark_procedure_list_should_return_list_containing_procedure_at_the_first_place(
        procedure_name=procedure_name,
        arguments="()",
        result_type="NUMBER",
    )

    _test_steps.snowpark_procedure_execute_should_return_expected_value(
        procedure_name=procedure_name,
        arguments="()",
        expected_value=1,
    )

    _test_steps.snowpark_procedure_drop_should_finish_successfully(
        procedure_name=procedure_name,
        arguments="()",
    )
    _test_steps.assert_that_no_procedures_are_in_snowflake()
    _test_steps.assert_that_only_these_files_are_staged_in_snowflake(
        f"deployments/{procedure_name}/app.zip"
    )

    _test_steps.snowpark_procedure_list_should_return_no_data()


@pytest.fixture
def _test_setup(
    runner,
    snowflake_session,
    sql_test_helper,
    object_name_provider,
    temporary_working_directory,
    snapshot,
):
    snowpark_procedure_test_setup = SnowparkTestSetup(
        runner=runner,
        snowflake_session=snowflake_session,
        sql_test_helper=sql_test_helper,
        object_name_provider=object_name_provider,
        snapshot=snapshot,
    )
    yield snowpark_procedure_test_setup
    snowpark_procedure_test_setup.clean_after_test_case()


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkTestSteps(_test_setup)

