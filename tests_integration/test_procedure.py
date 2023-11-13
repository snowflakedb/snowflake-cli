from __future__ import annotations

import json

import pytest

from tests_integration.snowflake_connector import snowflake_session, test_database
from tests_integration.testing_utils import assert_that_result_is_successful
from tests_integration.testing_utils.naming_utils import object_name_provider
from tests_integration.testing_utils.snowpark_utils import (
    SnowparkProcedureTestSteps,
    SnowparkTestSetup,
    TestType,
)
from tests_integration.testing_utils.sql_utils import sql_test_helper
from tests_integration.testing_utils.working_directory_utils import (
    temporary_working_directory,
)


@pytest.mark.integration
def test_snowpark_procedure_flow(
    _test_steps, temporary_working_directory_ctx, alter_snowflake_yml
):
    _test_steps.assert_that_no_entities_are_in_snowflake()
    _test_steps.assert_that_no_files_are_staged_in_test_db()

    _test_steps.snowpark_list_should_return_no_data()

    procedure_name = _test_steps.get_entity_name()

    with temporary_working_directory_ctx() as tmp_dir:
        _test_steps.snowpark_init_should_initialize_files_with_default_content()
        _test_steps.snowpark_package_should_zip_files()

        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="procedures.0.name",
            value=procedure_name,
        )
        result = _test_steps.run_deploy_2()
        assert_that_result_is_successful(result)

        _test_steps.assert_that_only_these_entities_are_in_snowflake(
            f"{procedure_name}(VARCHAR) RETURN VARCHAR"
        )

        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"deployments/{procedure_name}_name_string/app.zip"
        )

        _test_steps.snowpark_list_should_return_entity_at_first_place(
            entity_name=procedure_name,
            arguments="(VARCHAR)",
            result_type="VARCHAR",
        )

        _test_steps.snowpark_describe_should_return_entity_description(
            entity_name=procedure_name,
            arguments="(VARCHAR)",
            signature="(NAME VARCHAR)",
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            entity_name=procedure_name,
            arguments="('foo')",
            expected_value="Hello foo",
        )

        result = _test_steps.run_deploy_2()
        assert result.exit_code == 1
        assert "already exists" in result.output

        _test_steps.snowpark_execute_should_return_expected_value(
            entity_name=procedure_name,
            arguments="('foo')",
            expected_value="Hello foo",
        )

        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="procedures.0.returns",
            value="variant",
        )
        result = _test_steps.run_deploy_2("--replace")
        assert_that_result_is_successful(result)
        assert result.json == [
            {
                "object": f"{procedure_name}(name string)",
                "status": "definition updated",
                "type": "procedure",
            },
            {"object": "test()", "status": "packages updated", "type": "procedure"},
        ]

        _test_steps.assert_that_only_these_entities_are_in_snowflake(
            f"{procedure_name}(VARCHAR) RETURN VARIANT"
        )
        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"deployments/{procedure_name}_name_string/app.zip"
        )

        _test_steps.snowpark_list_should_return_entity_at_first_place(
            entity_name=procedure_name,
            arguments="(VARCHAR)",
            result_type="VARIANT",
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            entity_name=procedure_name,
            arguments="('foo')",
            expected_value='"Hello foo"',  # Because variant is returned
        )

        _test_steps.snowpark_drop_should_finish_successfully(
            entity_name=procedure_name,
            arguments="(varchar)",
        )
        _test_steps.assert_that_no_entities_are_in_snowflake()
        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"deployments/{procedure_name}_name_string/app.zip"
        )

        _test_steps.snowpark_list_should_return_no_data()


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
    snowpark_procedure_test_setup.clean_after_test_case()


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkProcedureTestSteps(_test_setup, TestType.PROCEDURE)
