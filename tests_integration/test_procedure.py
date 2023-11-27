from __future__ import annotations

from pathlib import Path
from time import sleep

import pytest

from tests_integration.testing_utils import assert_that_result_is_successful
from tests_integration.testing_utils.snowpark_utils import (
    SnowparkProcedureTestSteps,
    SnowparkTestSetup,
    TestType,
)

STAGE_NAME = "dev_deployment"


@pytest.mark.integration
def test_snowpark_procedure_flow(_test_steps, project_directory, alter_snowflake_yml):
    _test_steps.assert_no_procedures_in_snowflake()
    _test_steps.assert_no_functions_in_snowflake()

    _test_steps.assert_that_no_files_on_stage(stage_name=STAGE_NAME)

    _test_steps.object_show_should_return_no_data(object_type="function")
    _test_steps.object_show_should_return_no_data(object_type="procedure")

    procedure_name = _test_steps.get_entity_name()
    function_name = _test_steps.get_entity_name()

    with project_directory("snowpark") as tmp_dir:
        project_file: Path = tmp_dir / "snowflake.yml"
        _test_steps.snowpark_package_should_zip_files()

        alter_snowflake_yml(
            project_file,
            parameter_path="snowpark.procedures.0.name",
            value=procedure_name,
        )
        alter_snowflake_yml(
            project_file,
            parameter_path="snowpark.functions.0.name",
            value=function_name,
        )

        result = _test_steps.run_deploy()
        assert_that_result_is_successful(result)
        assert result.json == [
            {
                "object": f"{procedure_name}(name string)",
                "status": "created",
                "type": "procedure",
            },
            {"object": "test()", "status": "created", "type": "procedure"},
            {
                "object": f"{function_name}(name string)",
                "status": "created",
                "type": "function",
            },
        ]

        _test_steps.assert_those_procedures_are_in_snowflake(
            f"{procedure_name}(VARCHAR) RETURN VARCHAR"
        )
        _test_steps.assert_those_functions_are_in_snowflake(
            f"{function_name}(VARCHAR) RETURN VARCHAR"
        )

        expected_files = [
            f"{STAGE_NAME}/my_snowpark_project/app.zip",
        ]
        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )

        # Listing procedures or functions shows created objects
        _test_steps.object_show_includes_given_identifiers(
            object_type="procedure",
            identifier=(procedure_name, "(VARCHAR) RETURN VARCHAR"),
        )
        _test_steps.object_show_includes_given_identifiers(
            object_type="function",
            identifier=(function_name, "(VARCHAR) RETURN VARCHAR"),
        )

        # Created objects can be described
        _test_steps.object_describe_should_return_entity_description(
            object_type="procedure",
            identifier=f"{procedure_name}(VARCHAR)",
            signature="(NAME VARCHAR)",
            returns="VARCHAR(16777216)",
        )

        _test_steps.object_describe_should_return_entity_description(
            object_type="function",
            identifier=f"{function_name}(VARCHAR)",
            signature="(NAME VARCHAR)",
            returns="VARCHAR(16777216)",
        )

        # Created objects can be executed
        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier=f"{procedure_name}('foo')",
            expected_value="Hello foo",
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier=f"{function_name}('foo')",
            expected_value="Hello foo!",
        )

        # Subsequent deploy of same object should fail
        result = _test_steps.run_deploy()
        assert result.exit_code == 1
        assert "already exists" in result.output

        # Apply changes to project objects
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.procedures.0.returns",
            value="variant",
        )
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.0.returns",
            value="variant",
        )

        # Now we deploy with replace flag, it should update existing objects
        result = _test_steps.run_deploy("--replace")
        assert_that_result_is_successful(result)
        assert result.json == [
            {
                "object": f"{procedure_name}(name string)",
                "status": "definition updated",
                "type": "procedure",
            },
            {
                "object": "test()",
                "status": "packages updated",
                "type": "procedure",
            },
            {
                "object": f"{function_name}(name string)",
                "status": "definition updated",
                "type": "function",
            },
        ]

        # Check if objects were updated
        _test_steps.assert_those_procedures_are_in_snowflake(
            f"{procedure_name}(VARCHAR) RETURN VARIANT"
        )
        _test_steps.assert_those_functions_are_in_snowflake(
            f"{function_name}(VARCHAR) RETURN VARIANT"
        )

        # Same file should be present
        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )

        # Listing procedures or functions shows updated objects
        _test_steps.object_show_includes_given_identifiers(
            object_type="procedure",
            identifier=(procedure_name, "(VARCHAR) RETURN VARIANT"),
        )
        _test_steps.object_show_includes_given_identifiers(
            object_type="function",
            identifier=(function_name, "(VARCHAR) RETURN VARIANT"),
        )

        # Updated objects can be executed
        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier=f"{procedure_name}('foo')",
            expected_value='"Hello foo"',
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier=f"{function_name}('foo')",
            expected_value='"Hello foo!"',
        )

        # Check if objects can be dropped
        _test_steps.object_drop_should_finish_successfully(
            object_type="procedure", identifier=f"{procedure_name}(varchar)"
        )
        _test_steps.object_drop_should_finish_successfully(
            object_type="function", identifier=f"{function_name}(varchar)"
        )

        _test_steps.object_show_should_return_no_data(object_type="function")
        _test_steps.object_show_should_return_no_data(object_type="procedure")

        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
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
    snowpark_procedure_test_setup = SnowparkTestSetup(
        runner=runner,
        snowflake_session=snowflake_session,
        sql_test_helper=sql_test_helper,
        object_name_provider=object_name_provider,
        test_database=test_database,
        snapshot=snapshot,
        test_type=TestType.PROCEDURE,
    )
    yield snowpark_procedure_test_setup
    snowpark_procedure_test_setup.clean_after_test_case(stage_name=STAGE_NAME)


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkProcedureTestSteps(_test_setup)
