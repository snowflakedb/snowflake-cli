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
    _test_steps, alter_snowflake_yml, temporary_working_directory_ctx
):
    _test_steps.assert_that_no_entities_are_in_snowflake()
    _test_steps.assert_that_no_files_are_staged_in_test_db()

    _test_steps.snowpark_list_should_return_no_data()

    with temporary_working_directory_ctx() as tmp_dir:
        _test_steps.snowpark_init_should_initialize_files_with_default_content()
        _test_steps.add_parameters_to_procedure("name: int, b: str")
        _test_steps.add_requirements_to_requirements_txt(["coverage"])
        _test_steps.requirements_file_should_contain_coverage()
        _test_steps.snowpark_package_should_zip_files()
        procedure_name = _test_steps.get_entity_name()
        parameters = "(name int, b string)"
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="procedures",
            value=[
                {
                    "name": procedure_name,
                    "signature": [
                        {"name": "name", "type": "int"},
                        {"name": "b", "type": "string"},
                    ],
                    "returns": "string",
                    "handler": "app.hello",
                }
            ],
        )
        result = _test_steps.run_deploy_2("--install-coverage-wrapper")
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

        _test_steps.assert_that_only_these_entities_are_in_snowflake(
            f"{procedure_name}(NUMBER, VARCHAR) RETURN VARCHAR"
        )

        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"deployments/{stage_name}/app.zip"
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            entity_name=procedure_name,
            arguments="(0, 'test')",
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
