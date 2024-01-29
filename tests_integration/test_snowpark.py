from __future__ import annotations

from pathlib import Path

import pytest

from tests_integration.testing_utils import (
    SnowparkTestSteps,
)
from tests_integration.testing_utils.snowpark_utils import (
    SnowparkTestSetup,
)

STAGE_NAME = "dev_deployment"


@pytest.mark.integration
def test_snowpark_flow(_test_steps, project_directory, alter_snowflake_yml):
    with project_directory("snowpark") as tmp_dir:
        _test_steps.snowpark_build_should_zip_files()

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": "hello_procedure(name string)",
                    "status": "created",
                    "type": "procedure",
                },
                {"object": "test()", "status": "created", "type": "procedure"},
                {
                    "object": "hello_function(name string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        _test_steps.assert_those_procedures_are_in_snowflake(
            "HELLO_PROCEDURE(VARCHAR) RETURN VARCHAR"
        )
        _test_steps.assert_those_functions_are_in_snowflake(
            "HELLO_FUNCTION(VARCHAR) RETURN VARCHAR"
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
            identifier=("hello_procedure", "(VARCHAR) RETURN VARCHAR"),
        )
        _test_steps.object_show_includes_given_identifiers(
            object_type="function",
            identifier=("hello_function", "(VARCHAR) RETURN VARCHAR"),
        )

        # Created objects can be described
        _test_steps.object_describe_should_return_entity_description(
            object_type="procedure",
            identifier="HELLO_PROCEDURE(VARCHAR)",
            signature="(NAME VARCHAR)",
            returns="VARCHAR(16777216)",
        )

        _test_steps.object_describe_should_return_entity_description(
            object_type="function",
            identifier="HELLO_FUNCTION(VARCHAR)",
            signature="(NAME VARCHAR)",
            returns="VARCHAR(16777216)",
        )

        # Created objects can be executed
        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="hello_procedure('foo')",
            expected_value="Hello foo",
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="hello_function('foo')",
            expected_value="Hello foo!",
        )

        # Subsequent deploy of same object should fail
        _test_steps.snowpark_deploy_should_return_error_with_message_contains(
            "Following objects already exists"
        )

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
        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            additional_arguments=["--replace"],
            expected_result=[
                {
                    "object": "hello_procedure(name string)",
                    "status": "definition updated",
                    "type": "procedure",
                },
                {
                    "object": "test()",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": "hello_function(name string)",
                    "status": "definition updated",
                    "type": "function",
                },
            ],
        )

        # Check if objects were updated
        _test_steps.assert_those_procedures_are_in_snowflake(
            "HELLO_PROCEDURE(VARCHAR) RETURN VARIANT"
        )
        _test_steps.assert_those_functions_are_in_snowflake(
            "HELLO_FUNCTION(VARCHAR) RETURN VARIANT"
        )

        # Same file should be present
        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )

        # Listing procedures or functions shows updated objects
        _test_steps.object_show_includes_given_identifiers(
            object_type="procedure",
            identifier=("hello_procedure", "(VARCHAR) RETURN VARIANT"),
        )
        _test_steps.object_show_includes_given_identifiers(
            object_type="function",
            identifier=("hello_function", "(VARCHAR) RETURN VARIANT"),
        )

        # Updated objects can be executed
        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="hello_procedure('foo')",
            expected_value='"Hello foo"',
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="hello_function('foo')",
            expected_value='"Hello foo!"',
        )

        # Check if objects can be dropped
        _test_steps.object_drop_should_finish_successfully(
            object_type="procedure", identifier="hello_procedure(varchar)"
        )
        _test_steps.object_drop_should_finish_successfully(
            object_type="function", identifier="hello_function(varchar)"
        )

        _test_steps.object_show_should_return_no_data(
            object_type="function", object_prefix="hello"
        )
        _test_steps.object_show_should_return_no_data(
            object_type="procedure", object_prefix="hello"
        )

        _test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )


@pytest.mark.integration
def test_snowpark_with_separately_created_package(
    _test_steps, project_directory, alter_snowflake_yml
):

    _test_steps.package_should_build_proper_artifact(
        "dummy_pkg_for_tests", "dummy_pkg_for_tests/shrubbery.py"
    )
    _test_steps.package_should_upload_artifact_to_stage(
        "dummy_pkg_for_tests.zip", STAGE_NAME
    )

    _test_steps.artifacts_left_after_package_creation_should_be_deleted(
        "dummy_pkg_for_tests.zip"
    )

    with project_directory("snowpark_with_package"):
        _test_steps.snowpark_build_should_zip_files()

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": "test_func(name string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )
        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="test_func('foo')",
            expected_value="We want... a shrubbery!",
        )


@pytest.mark.integration
def test_snowpark_with_single_dependency_having_no_other_deps(
    runner, _test_steps, project_directory, alter_snowflake_yml
):

    with project_directory("snowpark_with_single_requirements_having_no_other_deps"):
        result = runner.invoke_json(
            [
                "snowpark",
                "build",
                "--pypi-download",
                "yes",
                "--check-anaconda-for-pypi-deps",
            ]
        )
        assert result.exit_code == 0

        packages_dir = Path(".packages")

        assert packages_dir.exists()
        assert (packages_dir / "dummy_pkg_for_tests").exists()

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": "test_func(name string)",
                    "type": "function",
                    "status": "created",
                }
            ]
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="test_func('foo')",
            expected_value="We want... a shrubbery!",
        )


@pytest.mark.integration
def test_snowpark_with_single_requirement_having_transient_deps(
    runner, _test_steps, project_directory, alter_snowflake_yml
):

    with project_directory("snowpark_with_single_requirements_having_transient_deps"):
        result = runner.invoke_json(
            [
                "snowpark",
                "build",
                "--pypi-download",
                "yes",
                "--check-anaconda-for-pypi-deps",
            ]
        )
        assert result.exit_code == 0

        packages_dir = Path(".packages")

        assert packages_dir.exists()
        assert (packages_dir / "dummy_pkg_for_tests_with_deps").exists()
        assert (packages_dir / "dummy_pkg_for_tests").exists()  # as transient dep

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": "test_func(name string)",
                    "type": "function",
                    "status": "created",
                }
            ]
        )

        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="test_func('foo')",
            expected_value="['We want... a shrubbery!', 'fishy, fishy, fish!']",
        )


@pytest.fixture
def _test_setup(
    runner,
    sql_test_helper,
    test_database,
    temporary_working_directory,
    snapshot,
):
    snowpark_procedure_test_setup = SnowparkTestSetup(
        runner=runner,
        sql_test_helper=sql_test_helper,
        test_database=test_database,
        snapshot=snapshot,
    )
    yield snowpark_procedure_test_setup


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkTestSteps(_test_setup)
