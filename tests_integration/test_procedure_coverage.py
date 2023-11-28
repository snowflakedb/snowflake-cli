import sys
from time import sleep

import pytest

from tests_integration.testing_utils.snowpark_utils import (
    SnowparkProcedureTestSteps,
    SnowparkTestSetup,
    TestType,
)

STAGE_NAME = "dev_deployment"


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Skip for windows, missing generated coverage files on stage",
)
@pytest.mark.integration
def test_procedure_coverage_flow(
    project_directory, _test_steps, alter_snowflake_yml, temporary_working_directory_ctx
):
    _test_steps.assert_no_procedures_in_snowflake()
    _test_steps.assert_no_functions_in_snowflake()

    _test_steps.assert_that_no_files_on_stage(stage_name=STAGE_NAME)

    _test_steps.object_show_should_return_no_data(object_type="procedure")

    with project_directory("snowpark_coverage") as tmp_dir:
        _test_steps.snowpark_package_should_zip_files()

        procedure_name = _test_steps.get_entity_name()
        parameters = "(name int, b string)"
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.procedures.0.name",
            value=procedure_name,
        )

        result = _test_steps.run_deploy("--install-coverage-wrapper")
        assert result.exit_code == 0
        assert result.json == [
            {
                "object": f"{procedure_name}(name int, b string)",
                "type": "procedure",
                "status": "created",
            }
        ]

        identifier = procedure_name + parameters

        _test_steps.assert_those_procedures_are_in_snowflake(
            f"{procedure_name}(NUMBER, VARCHAR) RETURN VARCHAR"
        )

        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"{STAGE_NAME}/my_snowpark_project/app.zip", stage_name=STAGE_NAME
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier=f"{procedure_name}(0, 'test')",
            expected_value="Hello 0",
        )

        _test_steps.assert_that_only_app_and_coverage_file_are_staged_in_test_db(
            stage_path=f"{STAGE_NAME}/my_snowpark_project",
            artifact_name="app.zip",
            stage_name=STAGE_NAME,
        )

        _test_steps.procedure_coverage_should_return_report_when_files_are_present_on_stage(
            identifier=identifier
        )

        _test_steps.coverage_clear_should_execute_successfully()

        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"{STAGE_NAME}/my_snowpark_project/app.zip", stage_name=STAGE_NAME
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
    procedure_coverage_test_setup.clean_after_test_case(stage_name=STAGE_NAME)


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkProcedureTestSteps(_test_setup)
