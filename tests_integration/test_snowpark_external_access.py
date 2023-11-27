import pytest

from tests_integration.testing_utils import (
    SnowparkProcedureTestSteps,
    SnowparkTestSetup,
    TestType,
    assert_that_result_is_successful,
)

STAGE_NAME = "dev_deployment"


@pytest.mark.integration
def test_snowpark_external_access(project_directory, _test_steps):

    with project_directory("snowpark_external_access"):
        _test_steps.snowpark_package_should_zip_files()

        result = _test_steps.run_deploy()
        assert_that_result_is_successful(result)
        assert result.json == [
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
    snowflake_session,
    sql_test_helper,
    object_name_provider,
    test_database,
):
    snowpark_test_setup = SnowparkTestSetup(
        runner=runner,
        snowflake_session=snowflake_session,
        sql_test_helper=sql_test_helper,
        object_name_provider=object_name_provider,
        test_database=test_database,
        snapshot=None,  # not needed
        test_type=None,  # not needed
    )
    yield snowpark_test_setup
    snowpark_test_setup.clean_after_test_case(stage_name=STAGE_NAME)


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkProcedureTestSteps(_test_setup)
