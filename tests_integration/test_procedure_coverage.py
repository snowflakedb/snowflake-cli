import sys

import pytest

from tests_integration.testing_utils.snowpark_utils import (
    SnowparkTestSetup,
    SnowparkTestSteps,
)

STAGE_NAME = "dev_deployment"


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Skip for windows, missing generated coverage files on stage",
)
@pytest.mark.integration
def test_procedure_coverage_flow(project_directory, _test_steps):
    with project_directory("snowpark_coverage"):
        _test_steps.snowpark_build_should_zip_files()

        _test_steps.snowpark_deploy_with_coverage_wrapper_should_finish_successfully_and_return(
            [
                {
                    "object": "hello(name int, b string)",
                    "type": "procedure",
                    "status": "created",
                }
            ]
        )

        _test_steps.assert_those_procedures_are_in_snowflake(
            "hello", "HELLO(NUMBER, VARCHAR) RETURN VARCHAR"
        )

        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"{STAGE_NAME}/my_snowpark_project/app.zip", stage_name=STAGE_NAME
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="hello(0, 'test')",
            expected_value="Hello 0",
        )

        _test_steps.assert_that_only_app_and_coverage_file_are_staged_in_test_db(
            stage_path=f"{STAGE_NAME}/my_snowpark_project",
            artifact_name="app.zip",
            stage_name=STAGE_NAME,
        )

        _test_steps.procedure_coverage_should_return_report_when_files_are_present_on_stage(
            identifier="hello(name int, b string)"
        )

        _test_steps.coverage_clear_should_execute_successfully()

        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"{STAGE_NAME}/my_snowpark_project/app.zip", stage_name=STAGE_NAME
        )


@pytest.fixture
def _test_setup(
    runner,
    sql_test_helper,
    test_database,
    temporary_working_directory,
    snapshot,
):
    procedure_coverage_test_setup = SnowparkTestSetup(
        runner=runner,
        sql_test_helper=sql_test_helper,
        test_database=test_database,
        snapshot=snapshot,
    )

    yield procedure_coverage_test_setup


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkTestSteps(_test_setup)
