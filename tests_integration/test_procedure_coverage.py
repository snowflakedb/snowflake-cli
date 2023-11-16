import pytest
import sys

from tests_integration.snowflake_connector import snowflake_session, test_database
from tests_integration.testing_utils.naming_utils import object_name_provider
from tests_integration.testing_utils.snowpark_utils import (
    SnowparkTestSetup,
    TestType,
    SnowparkProcedureTestSteps,
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
def test_procedure_coverage_flow(
    project_directory, _test_steps, alter_snowflake_yml, temporary_working_directory_ctx
):
    _test_steps.assert_no_procedures_in_snowflake()
    _test_steps.assert_no_functions_in_snowflake()

    _test_steps.assert_that_no_files_are_staged_in_test_db()

    _test_steps.snowpark_list_should_return_no_data(object_type="procedure")

    with project_directory("snowpark_coverage") as tmp_dir:
        _test_steps.snowpark_package_should_zip_files()

        procedure_name = _test_steps.get_entity_name()
        parameters = "(name int, b string)"
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="procedures.0.name",
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
        stage_name = f"{procedure_name}_name_int_b_string"

        _test_steps.assert_those_procedures_are_in_snowflake(
            f"{procedure_name}(NUMBER, VARCHAR) RETURN VARCHAR"
        )

        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"deployments/{stage_name}/app.zip"
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier=f"{procedure_name}(0, 'test')",
            expected_value="Hello 0",
        )

        _test_steps.assert_that_only_app_and_coverage_file_are_staged_in_test_db(
            f"deployments/{stage_name}"
        )

        _test_steps.procedure_coverage_should_return_report_when_files_are_present_on_stage(
            identifier=identifier
        )

        _test_steps.coverage_clear_should_execute_successfully(identifier=identifier)

        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"deployments/{stage_name}/app.zip"
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
    yield SnowparkProcedureTestSteps(_test_setup, TestType.PROCEDURE)
