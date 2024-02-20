from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests_integration.testing_utils import (
    SnowparkTestSteps,
    assert_that_result_is_successful,
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


@pytest.mark.integration
def test_snowpark_default_arguments(
    _test_steps, project_directory, alter_snowflake_yml
):
    with project_directory("snowpark_with_default_values") as tmp_dir:
        _test_steps.snowpark_build_should_zip_files()

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": "whole_new_word_procedure(base varchar default 'word', "
                    "mult number default 2, suffix varchar default ', but a procedure')",
                    "type": "procedure",
                    "status": "created",
                },
                {
                    "object": "whole_new_word(base string default 'word', "
                    "mult int default 2, suffix string default '!')",
                    "type": "function",
                    "status": "created",
                },
                {
                    "object": "check_all_types("
                    "s string default '<str>', "
                    "i int default 7, "
                    "b1 boolean default true, "
                    "b2 boolean default True, "
                    "f float default 1.5, "
                    "l array default [1, 2, 3])",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        _test_steps.object_show_includes_given_identifiers(
            object_type="function",
            identifier=(
                "WHOLE_NEW_WORD",
                "( [VARCHAR] [, NUMBER] [, VARCHAR]) RETURN VARCHAR",
            ),
        )
        _test_steps.object_show_includes_given_identifiers(
            object_type="procedure",
            identifier=(
                "WHOLE_NEW_WORD_PROCEDURE",
                "( [VARCHAR] [, NUMBER] [, VARCHAR]) RETURN VARCHAR",
            ),
        )

        # Created objects can be described
        _test_steps.object_describe_should_return_entity_description(
            object_type="function",
            identifier="WHOLE_NEW_WORD(VARCHAR, NUMBER, VARCHAR)",
            signature="(BASE VARCHAR, MULT NUMBER, SUFFIX VARCHAR)",
            returns="VARCHAR(16777216)",
        )
        _test_steps.object_describe_should_return_entity_description(
            object_type="procedure",
            identifier="WHOLE_NEW_WORD_PROCEDURE(VARCHAR, NUMBER, VARCHAR)",
            signature="(BASE VARCHAR, MULT NUMBER, SUFFIX VARCHAR)",
            returns="VARCHAR(16777216)",
        )

        # execute with default arguments
        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="whole_new_word()",
            expected_value="wordword!",
        )
        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="whole_new_word_procedure()",
            expected_value="wordword, but a procedure",
        )

        # execute naming arguments
        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="whole_new_word(mult => 4, base => 'nii')",
            expected_value="niiniiniinii!",
        )
        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="whole_new_word_procedure(mult => 4, base => 'nii')",
            expected_value="niiniiniinii, but a procedure",
        )

        # check default values for all types
        _test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="check_all_types()",
            expected_value="s:<str>, i:7, b1:True, b2:True, f:1.5, l:[1, 2, 3]",
        )


CUSTOM_SCHEMA = "custom_schema"
DIFFERENT_SCHEMA = "different_schema"


@pytest.fixture
def _create_custom_schemas(runner, test_database):
    for schema in [CUSTOM_SCHEMA, DIFFERENT_SCHEMA]:
        runner.invoke_with_connection(
            ["sql", "-q", f"create schema {test_database}.{schema}"]
        )


@pytest.mark.skip(reason="Weird error")
@pytest.mark.integration
def test_snowpark_deploy_with_defined_stage(
    _test_steps, _create_custom_schemas, runner, multiple_project_directories
):
    different_schema = "totally_different_schema"
    custom_schema = "custom_schema"

    def invoke_from_workdir_connection(path, command):
        os.chdir(path)
        result = runner.invoke_with_connection_json(command)
        assert_that_result_is_successful(result)
        return result

    def invoke_from_workdir(path, command):
        os.chdir(path)
        result = runner.invoke(command)
        assert_that_result_is_successful(result)
        return result

    def _assert_streamlit_exists(schema=None):
        cmd = ["object", "list", "streamlit"]
        if schema is not None:
            cmd += ["--schema", schema]
        result = runner.invoke_with_connection_json(cmd)
        assert_that_result_is_successful(result)
        assert len(result.json) == 1
        assert result.json[0]["schema_name"] == schema.upper() if schema else "PUBLIC"

    with multiple_project_directories("snowpark", "snowpark_with_defined_schema") as (
        snowpark_public,
        snowpark_custom,
    ):
        for project in [snowpark_public, snowpark_custom]:
            invoke_from_workdir(project, ["snowpark", "build"])

        # snowparks from different schemas should not conflict
        result = invoke_from_workdir(snowpark_public, ["snowpark", "deploy"])
        print(result.json)
        # invoke_from_workdir(
        #     streamlit_public, ["streamlit", "deploy", "--schema", different_schema]
        # )
        # invoke_from_workdir(streamlit_custom, ["streamlit", "deploy"])
        #
        # # streamlits from different schemas are independent - list shows only one
        # _assert_streamlit_exists()
        # _assert_streamlit_exists(different_schema)
        # _assert_streamlit_exists(custom_schema)


@pytest.mark.integration
def test_snowpark_fqn(
    _test_steps,
    test_database,
    project_directory,
    alter_snowflake_yml,
    _create_custom_schemas,
):
    database = test_database.upper()
    schema = "PUBLIC"

    with project_directory("snowpark_with_defined_schema") as tmp_dir:
        _test_steps.snowpark_build_should_zip_files()

        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.0.name",
            value=f"{database}.{schema}.fqn_function",
        )
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.1.schema",
            value=DIFFERENT_SCHEMA,
        )
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.2.database",
            value=database,
        )
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.3.schema",
            value=DIFFERENT_SCHEMA,
        )
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.3.database",
            value=database,
        )

        print((tmp_dir / "snowflake.yml").read_text())

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.PUBLIC.FQN_FUNCTION(name string)",
                    "status": "created",
                    "type": "function",
                },
                {
                    "object": f"{database}.DIFFERENT_SCHEMA.SCHEMA_FUNCTION(name "
                    "string)",
                    "status": "created",
                    "type": "function",
                },
                {
                    "object": f"{database}.PUBLIC.DATABASE_FUNCTION(name string)",
                    "status": "created",
                    "type": "function",
                },
                {
                    "object": f"{database}.DIFFERENT_SCHEMA.DATABASE_SCHEMA_FUNCTION(name "
                    "string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        _test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.PUBLIC.FQN_FUNCTION(name string)",
                    "status": "packages updated",
                    "type": "function",
                },
                {
                    "object": f"{database}.DIFFERENT_SCHEMA.SCHEMA_FUNCTION(name "
                    "string)",
                    "status": "packages updated",
                    "type": "function",
                },
                {
                    "object": f"{database}.PUBLIC.DATABASE_FUNCTION(name string)",
                    "status": "packages updated",
                    "type": "function",
                },
                {
                    "object": f"{database}.DIFFERENT_SCHEMA.DATABASE_SCHEMA_FUNCTION(name "
                    "string)",
                    "status": "packages updated",
                    "type": "function",
                },
            ],
            additional_arguments=["--replace"],
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
