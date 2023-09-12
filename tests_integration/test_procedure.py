from __future__ import annotations

import pytest

from tests_integration.snowflake_connector import snowflake_session, test_database
from tests_integration.testing_utils.naming_utils import object_name_provider
from tests_integration.testing_utils.snowpark_utils import (
    SnowparkTestSteps,
    SnowparkTestSetup,
    TestType,
)
from tests_integration.testing_utils.sql_utils import sql_test_helper
from tests_integration.testing_utils.working_directory_utils import (
    temporary_working_directory,
)


@pytest.mark.integration
def test_snowpark_procedure_flow(_test_steps):
    _test_steps.assert_that_no_entities_are_in_snowflake()
    _test_steps.assert_that_no_files_are_staged_in_test_db()

    _test_steps.snowpark_list_should_return_no_data()

    _test_steps.snowpark_init_should_initialize_files_with_default_content()
    _test_steps.snowpark_package_should_zip_files()

    procedure_name = _test_steps.snowpark_create_should_finish_successfully()
    _test_steps.assert_that_only_these_entities_are_in_snowflake(
        f"{procedure_name}() RETURN VARCHAR"
    )
    _test_steps.assert_that_only_these_files_are_staged_in_test_db(
        f"deployments/{procedure_name}/app.zip"
    )

    _test_steps.snowpark_list_should_return_entity_at_first_place(
        entity_name=procedure_name,
        arguments="()",
        result_type="VARCHAR",
    )

    _test_steps.snowpark_describe_should_return_entity_description(
        entity_name=procedure_name,
        arguments="()",
    )

    _test_steps.snowpark_execute_should_return_expected_value(
        entity_name=procedure_name,
        arguments="()",
        expected_value="Hello World!",
    )

    _test_steps.snowpark_update_should_not_replace_if_the_signature_does_not_change(
        procedure_name
    )
    _test_steps.snowpark_execute_should_return_expected_value(
        entity_name=procedure_name,
        arguments="()",
        expected_value="Hello Snowflakes!",
    )

    _test_steps.snowpark_update_should_finish_successfully(procedure_name)
    _test_steps.assert_that_only_these_entities_are_in_snowflake(
        f"{procedure_name}() RETURN NUMBER"
    )
    _test_steps.assert_that_only_these_files_are_staged_in_test_db(
        f"deployments/{procedure_name}/app.zip"
    )

    _test_steps.snowpark_list_should_return_entity_at_first_place(
        entity_name=procedure_name,
        arguments="()",
        result_type="NUMBER",
    )

    _test_steps.snowpark_execute_should_return_expected_value(
        entity_name=procedure_name,
        arguments="()",
        expected_value=1,
    )

    _test_steps.snowpark_drop_should_finish_successfully(
        entity_name=procedure_name,
        arguments="()",
    )
    _test_steps.assert_that_no_entities_are_in_snowflake()
    _test_steps.assert_that_only_these_files_are_staged_in_test_db(
        f"deployments/{procedure_name}/app.zip"
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
    yield SnowparkTestSteps(_test_setup, TestType.PROCEDURE)
