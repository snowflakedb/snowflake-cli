import pytest

from tests_integration.testing_utils import SnowparkTestSetup, SnowparkTestSteps

STAGE_NAME = "dev_deployment"


@pytest.mark.integration
def test_snowpark_external_access(project_directory, _test_steps):

    with project_directory("snowpark_external_access"):
        _test_steps.snowpark_build_should_zip_files()

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": "status_procedure()",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": "status_function()",
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
