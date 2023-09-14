import pytest
import sys

from tests_integration.snowflake_connector import snowflake_session, test_database
from tests_integration.testing_utils.naming_utils import object_name_provider
from tests_integration.testing_utils.snowpark_utils import (
    SnowparkTestSetup,
    TestType,
    SnowparkTestSteps,
)
from tests_integration.testing_utils.sql_utils import sql_test_helper
from tests_integration.testing_utils.working_directory_utils import (
    temporary_working_directory,
)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Skip for windows, missing generated coverage files on stage",
)
@pytest.mark.integration
def test_procedure_coverage_flow(_test_steps):
    _test_steps.assert_that_no_entities_are_in_snowflake()
    _test_steps.assert_that_no_files_are_staged_in_test_db()

    _test_steps.snowpark_list_should_return_no_data()

    _test_steps.snowpark_init_should_initialize_files_with_default_content()
    _test_steps.add_requirements_to_requirements_txt(["coverage"])
    _test_steps.requirements_file_should_contain_coverage()
    _test_steps.snowpark_package_should_zip_files()

    procedure_name = (
        _test_steps.snowpark_create_with_coverage_wrapper_should_finish_succesfully()
    )

    _test_steps.assert_that_only_these_entities_are_in_snowflake(
        f"{procedure_name}() RETURN VARCHAR"
    )

    _test_steps.assert_that_only_these_files_are_staged_in_test_db(
        f"deployments/{procedure_name}/app.zip"
    )

    _test_steps.snowpark_execute_should_return_expected_value(
        entity_name=procedure_name,
        arguments="()",
        expected_value="Hello World!",
    )

    _test_steps.assert_that_only_app_and_coverage_file_are_staged_in_test_db(
        f"deployments/{procedure_name}"
    )

    _test_steps.procedure_coverage_should_return_report_when_files_are_present_on_stage(
        procedure_name=procedure_name, arguments="()"
    )

    _test_steps.coverage_clear_should_execute_succesfully(
        procedure_name=procedure_name, arguments="()"
    )

    _test_steps.assert_that_only_these_files_are_staged_in_test_db(
        f"deployments/{procedure_name}/app.zip"
    )


@pytest.fixture
def _test_setup(
    runner,
    snowflake_session,
    sql_test_helper,
    object_name_provider,
    test_database,
    temporary_working_directory,
    snapshot,
):
    procedure_coverage_test_setup = SnowparkTestSetup(
        runner=runner,
        snowflake_session=snowflake_session,
        sql_test_helper=sql_test_helper,
        object_name_provider=object_name_provider,
        test_database=test_database,
        snapshot=snapshot,
        test_type=TestType.PROCEDURE,
    )

    yield procedure_coverage_test_setup
    procedure_coverage_test_setup.clean_after_test_case()


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkTestSteps(_test_setup, TestType.PROCEDURE)
