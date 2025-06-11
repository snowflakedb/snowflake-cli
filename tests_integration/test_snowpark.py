# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import sys
from pathlib import Path
from textwrap import dedent

import pytest

from tests_integration.conftest import IS_QA
from tests_integration.testing_utils import (
    SnowparkTestSteps,
)
from tests_integration.testing_utils.snowpark_utils import (
    FlowTestSetup,
)
from typing import List
from zipfile import ZipFile


STAGE_NAME = "dev_deployment"
RETURN_TYPE = "VARCHAR"
bundle_root = Path("output") / "bundle" / "snowpark"


@pytest.mark.integration
def test_snowpark_flow(
    _snowpark_test_steps,
    project_directory,
    alter_snowflake_yml,
    test_database,
    enable_snowpark_glob_support_feature_flag,
):
    database = test_database.upper()
    with project_directory("snowpark") as tmp_dir:
        _snowpark_test_steps.snowpark_build_should_zip_files(
            additional_files=[
                Path("output"),
                Path("output") / "bundle",
                bundle_root,
                bundle_root / "my_snowpark_project",
                bundle_root / "my_snowpark_project" / "app.zip",
            ]
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        _snowpark_test_steps.assert_those_procedures_are_in_snowflake(
            "hello_procedure(VARCHAR) RETURN VARCHAR"
        )
        _snowpark_test_steps.assert_those_functions_are_in_snowflake(
            "hello_function(VARCHAR) RETURN VARCHAR"
        )

        expected_files = [
            f"{STAGE_NAME}/my_snowpark_project/app.zip",
            f"{STAGE_NAME}/dependencies.zip",
        ]
        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )

        # Listing procedures or functions shows created objects
        _snowpark_test_steps.object_show_includes_given_identifiers(
            object_type="procedure",
            identifier=("hello_procedure", "(VARCHAR) RETURN VARCHAR"),
        )
        _snowpark_test_steps.object_show_includes_given_identifiers(
            object_type="function",
            identifier=("hello_function", "(VARCHAR) RETURN VARCHAR"),
        )

        # Created objects can be described
        _snowpark_test_steps.object_describe_should_return_entity_description(
            object_type="procedure",
            identifier="hello_procedure(VARCHAR)",
            signature="(NAME VARCHAR)",
            returns=RETURN_TYPE,
        )

        _snowpark_test_steps.object_describe_should_return_entity_description(
            object_type="function",
            identifier="hello_function(VARCHAR)",
            signature="(NAME VARCHAR)",
            returns=RETURN_TYPE,
        )

        # Grants are given correctly

        _snowpark_test_steps.set_grants_on_selected_object(
            object_type="procedure",
            object_name="hello_procedure(VARCHAR)",
            privillege="USAGE",
            role="test_role",
        )

        _snowpark_test_steps.set_grants_on_selected_object(
            object_type="function",
            object_name="hello_function(VARCHAR)",
            privillege="USAGE",
            role="test_role",
        )

        _snowpark_test_steps.assert_that_object_has_expected_grant(
            object_type="procedure",
            object_name="hello_procedure(VARCHAR)",
            expected_privillege="USAGE",
            expected_role="test_role",
        )

        _snowpark_test_steps.assert_that_object_has_expected_grant(
            object_type="function",
            object_name="hello_function(VARCHAR)",
            expected_privillege="USAGE",
            expected_role="test_role",
        )

        # Created objects can be executed
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="hello_procedure('foo')",
            expected_value="Hello foo",
        )

        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="hello_function('foo')",
            expected_value="Hello foo!",
        )

        # Subsequent deploy of same object should fail
        _snowpark_test_steps.snowpark_deploy_should_return_error_with_message_contains(
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
        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            additional_arguments=["--replace"],
            expected_result=[
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "definition updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "definition updated",
                    "type": "function",
                },
            ],
        )

        # Apply another changes to project objects
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.procedures.0.execute_as_caller",
            value="true",
        )
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.0.runtime",
            value="3.11",
        )

        # Another deploy with replace flag, it should update existing objects
        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            additional_arguments=["--replace"],
            expected_result=[
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "definition updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "definition updated",
                    "type": "function",
                },
            ],
        )

        # Try to deploy again, with --force-replace flag, all objects should be updated
        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            additional_arguments=["--force-replace"],
            expected_result=[
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "created",
                    "type": "function",
                },
            ],
        )

        # Check if objects were updated
        _snowpark_test_steps.assert_those_procedures_are_in_snowflake(
            "hello_procedure(VARCHAR) RETURN VARIANT"
        )
        _snowpark_test_steps.assert_those_functions_are_in_snowflake(
            "hello_function(VARCHAR) RETURN VARIANT"
        )

        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )

        # Listing procedures or functions shows updated objects
        _snowpark_test_steps.object_show_includes_given_identifiers(
            object_type="procedure",
            identifier=("hello_procedure", "(VARCHAR) RETURN VARIANT"),
        )
        _snowpark_test_steps.object_show_includes_given_identifiers(
            object_type="function",
            identifier=("hello_function", "(VARCHAR) RETURN VARIANT"),
        )

        # Updated objects can be executed
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="hello_procedure('foo')",
            expected_value='"Hello foo"',
        )

        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="hello_function('foo')",
            expected_value='"Hello foo!"',
        )

        # Check if adding import triggers replace
        _snowpark_test_steps.package_should_build_proper_artifact(
            "dummy_pkg_for_tests", "dummy_pkg_for_tests/shrubbery.py"
        )
        _snowpark_test_steps.package_should_upload_artifact_to_stage(
            "dummy_pkg_for_tests.zip", STAGE_NAME
        )

        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.0.imports",
            value=["@dev_deployment/dummy_pkg_for_tests.zip"],
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            additional_arguments=["--replace"],
            expected_result=[
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "definition updated",
                    "type": "function",
                },
            ],
        )

        # Same file should be present, with addition of uploaded package
        expected_files.append(f"{STAGE_NAME}/dummy_pkg_for_tests.zip")

        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )

        # Grants are preserved after updates

        _snowpark_test_steps.assert_that_object_has_expected_grant(
            object_type="procedure",
            object_name="hello_procedure(VARCHAR)",
            expected_privillege="USAGE",
            expected_role="test_role",
        )

        _snowpark_test_steps.assert_that_object_has_expected_grant(
            object_type="function",
            object_name="hello_function(VARCHAR)",
            expected_privillege="USAGE",
            expected_role="test_role",
        )

        # Check if objects can be dropped
        _snowpark_test_steps.object_drop_should_finish_successfully(
            object_type="procedure", identifier="hello_procedure(varchar)"
        )
        _snowpark_test_steps.object_drop_should_finish_successfully(
            object_type="function", identifier="hello_function(varchar)"
        )

        _snowpark_test_steps.object_show_should_return_no_data(
            object_type="function", object_prefix="hello"
        )
        _snowpark_test_steps.object_show_should_return_no_data(
            object_type="procedure", object_prefix="hello"
        )

        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )


@pytest.mark.integration
def test_snowpark_flow_old_build(
    _snowpark_test_steps, project_directory, alter_snowflake_yml, test_database
):
    database = test_database.upper()
    with project_directory("snowpark") as tmp_dir:
        _snowpark_test_steps.snowpark_build_should_zip_files(
            additional_files=[Path("app.zip")]
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        _snowpark_test_steps.assert_those_procedures_are_in_snowflake(
            "hello_procedure(VARCHAR) RETURN VARCHAR"
        )
        _snowpark_test_steps.assert_those_functions_are_in_snowflake(
            "hello_function(VARCHAR) RETURN VARCHAR"
        )

        expected_files = [
            f"{STAGE_NAME}/my_snowpark_project/app.zip",
            f"{STAGE_NAME}/dependencies.zip",
        ]
        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )

        # Listing procedures or functions shows created objects
        _snowpark_test_steps.object_show_includes_given_identifiers(
            object_type="procedure",
            identifier=("hello_procedure", "(VARCHAR) RETURN VARCHAR"),
        )
        _snowpark_test_steps.object_show_includes_given_identifiers(
            object_type="function",
            identifier=("hello_function", "(VARCHAR) RETURN VARCHAR"),
        )

        # Created objects can be described
        _snowpark_test_steps.object_describe_should_return_entity_description(
            object_type="procedure",
            identifier="hello_procedure(VARCHAR)",
            signature="(NAME VARCHAR)",
            returns=RETURN_TYPE,
        )

        _snowpark_test_steps.object_describe_should_return_entity_description(
            object_type="function",
            identifier="hello_function(VARCHAR)",
            signature="(NAME VARCHAR)",
            returns=RETURN_TYPE,
        )

        # Grants are given correctly

        _snowpark_test_steps.set_grants_on_selected_object(
            object_type="procedure",
            object_name="hello_procedure(VARCHAR)",
            privillege="USAGE",
            role="test_role",
        )

        _snowpark_test_steps.set_grants_on_selected_object(
            object_type="function",
            object_name="hello_function(VARCHAR)",
            privillege="USAGE",
            role="test_role",
        )

        _snowpark_test_steps.assert_that_object_has_expected_grant(
            object_type="procedure",
            object_name="hello_procedure(VARCHAR)",
            expected_privillege="USAGE",
            expected_role="test_role",
        )

        _snowpark_test_steps.assert_that_object_has_expected_grant(
            object_type="function",
            object_name="hello_function(VARCHAR)",
            expected_privillege="USAGE",
            expected_role="test_role",
        )

        # Created objects can be executed
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="hello_procedure('foo')",
            expected_value="Hello foo",
        )

        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="hello_function('foo')",
            expected_value="Hello foo!",
        )

        # Subsequent deploy of same object should fail
        _snowpark_test_steps.snowpark_deploy_should_return_error_with_message_contains(
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
        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            additional_arguments=["--replace"],
            expected_result=[
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "definition updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "definition updated",
                    "type": "function",
                },
            ],
        )

        # Apply another changes to project objects
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.procedures.0.execute_as_caller",
            value="true",
        )
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.0.runtime",
            value="3.11",
        )

        # Another deploy with replace flag, it should update existing objects
        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            additional_arguments=["--replace"],
            expected_result=[
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "definition updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "definition updated",
                    "type": "function",
                },
            ],
        )

        # Check if objects were updated
        _snowpark_test_steps.assert_those_procedures_are_in_snowflake(
            "hello_procedure(VARCHAR) RETURN VARIANT"
        )
        _snowpark_test_steps.assert_those_functions_are_in_snowflake(
            "hello_function(VARCHAR) RETURN VARIANT"
        )

        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )

        # Listing procedures or functions shows updated objects
        _snowpark_test_steps.object_show_includes_given_identifiers(
            object_type="procedure",
            identifier=("hello_procedure", "(VARCHAR) RETURN VARIANT"),
        )
        _snowpark_test_steps.object_show_includes_given_identifiers(
            object_type="function",
            identifier=("hello_function", "(VARCHAR) RETURN VARIANT"),
        )

        # Updated objects can be executed
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="hello_procedure('foo')",
            expected_value='"Hello foo"',
        )

        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="hello_function('foo')",
            expected_value='"Hello foo!"',
        )

        # Check if adding import triggers replace
        _snowpark_test_steps.package_should_build_proper_artifact(
            "dummy_pkg_for_tests", "dummy_pkg_for_tests/shrubbery.py"
        )
        _snowpark_test_steps.package_should_upload_artifact_to_stage(
            "dummy_pkg_for_tests.zip", STAGE_NAME
        )

        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.0.imports",
            value=["@dev_deployment/dummy_pkg_for_tests.zip"],
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            additional_arguments=["--replace"],
            expected_result=[
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "packages updated",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "definition updated",
                    "type": "function",
                },
            ],
        )

        # Same file should be present, with addition of uploaded package
        expected_files.append(f"{STAGE_NAME}/dummy_pkg_for_tests.zip")

        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )

        # Grants are preserved after updates

        _snowpark_test_steps.assert_that_object_has_expected_grant(
            object_type="procedure",
            object_name="hello_procedure(VARCHAR)",
            expected_privillege="USAGE",
            expected_role="test_role",
        )

        _snowpark_test_steps.assert_that_object_has_expected_grant(
            object_type="function",
            object_name="hello_function(VARCHAR)",
            expected_privillege="USAGE",
            expected_role="test_role",
        )

        # Check if objects can be dropped
        _snowpark_test_steps.object_drop_should_finish_successfully(
            object_type="procedure", identifier="hello_procedure(varchar)"
        )
        _snowpark_test_steps.object_drop_should_finish_successfully(
            object_type="function", identifier="hello_function(varchar)"
        )

        _snowpark_test_steps.object_show_should_return_no_data(
            object_type="function", object_prefix="hello"
        )
        _snowpark_test_steps.object_show_should_return_no_data(
            object_type="procedure", object_prefix="hello"
        )

        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            *expected_files, stage_name=STAGE_NAME
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "project_name", ["snowpark_with_import_v1", "snowpark_with_import_v2"]
)
def test_snowpark_with_separately_created_package(
    _snowpark_test_steps,
    project_directory,
    alter_snowflake_yml,
    test_database,
    project_name,
):
    _snowpark_test_steps.package_should_build_proper_artifact(
        "dummy_pkg_for_tests", "dummy_pkg_for_tests/shrubbery.py"
    )
    _snowpark_test_steps.package_should_upload_artifact_to_stage(
        "dummy_pkg_for_tests.zip", STAGE_NAME
    )

    _snowpark_test_steps.artifacts_left_after_package_creation_should_be_deleted(
        "dummy_pkg_for_tests.zip"
    )

    with project_directory(project_name):

        _snowpark_test_steps.snowpark_build_should_zip_files(
            additional_files=[Path("app.zip")], no_dependencies=True
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{test_database.upper()}.PUBLIC.test_func(name string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="test_func('foo')",
            expected_value="We want... a shrubbery!",
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "project_name",
    [
        "snowpark_with_single_requirements_having_no_other_deps_v1",
        "snowpark_with_single_requirements_having_no_other_deps_v2",
    ],
)
def test_snowpark_with_single_dependency_having_no_other_deps(
    runner,
    _snowpark_test_steps,
    project_directory,
    alter_snowflake_yml,
    test_database,
    project_name,
):
    with project_directory(project_name):
        result = runner.invoke_with_connection_json(["snowpark", "build"])
        assert result.exit_code == 0

        assert (
            "dummy_pkg_for_tests/shrubbery.py" in ZipFile("dependencies.zip").namelist()
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{test_database.upper()}.PUBLIC.test_func(name string)",
                    "type": "function",
                    "status": "created",
                }
            ]
        )

        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="test_func('foo')",
            expected_value="We want... a shrubbery!",
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "project_name",
    [
        "snowpark_with_single_requirements_having_transient_deps_v1",
        "snowpark_with_single_requirements_having_transient_deps_v2",
    ],
)
def test_snowpark_with_single_requirement_having_transient_deps(
    runner,
    _snowpark_test_steps,
    project_directory,
    alter_snowflake_yml,
    test_database,
    project_name,
):
    with project_directory(project_name):
        result = runner.invoke_with_connection_json(["snowpark", "build"])
        assert result.exit_code == 0

        files = ZipFile("dependencies.zip").namelist()
        assert "dummy_pkg_for_tests_with_deps/shrubbery.py" in files
        assert "dummy_pkg_for_tests/shrubbery.py" in files  # as transient dep

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{test_database.upper()}.PUBLIC.test_func(name string)",
                    "type": "function",
                    "status": "created",
                }
            ]
        )

        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="test_func('foo')",
            expected_value="['We want... a shrubbery!', 'fishy, fishy, fish!']",
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "project_name",
    [
        "snowpark_with_single_requirements_having_transient_deps_v1",
        "snowpark_with_single_requirements_having_transient_deps_v2",
    ],
)
def test_snowpark_commands_executed_outside_project_dir(
    runner,
    _snowpark_test_steps,
    project_directory,
    alter_snowflake_yml,
    test_database,
    project_name,
):
    project_subpath = "my_snowpark_project"
    with project_directory(
        project_name,
        subpath=project_subpath,
    ):
        result = runner.invoke_with_connection_json(
            ["snowpark", "build", "--project", project_subpath]
        )
        assert result.exit_code == 0

        files = ZipFile(Path(project_subpath) / "dependencies.zip").namelist()
        assert "dummy_pkg_for_tests_with_deps/shrubbery.py" in files
        assert "dummy_pkg_for_tests/shrubbery.py" in files  # as transient dep

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            additional_arguments=["--project", project_subpath],
            expected_result=[
                {
                    "object": f"{test_database.upper()}.PUBLIC.test_func(name string)",
                    "type": "function",
                    "status": "created",
                }
            ],
        )

        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="test_func('foo')",
            expected_value="['We want... a shrubbery!', 'fishy, fishy, fish!']",
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "project_name",
    ["snowpark_with_default_values_v1", "snowpark_with_default_values_v2"],
)
def test_snowpark_default_arguments(
    _snowpark_test_steps,
    project_directory,
    alter_snowflake_yml,
    test_database,
    project_name,
):
    database = test_database.upper()
    with project_directory(project_name):
        _snowpark_test_steps.snowpark_build_should_zip_files(
            additional_files=[Path("app.zip")], no_dependencies=True
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.PUBLIC.whole_new_word_procedure(base varchar default 'word', "
                    "mult number default 2, suffix varchar default ', but a procedure')",
                    "type": "procedure",
                    "status": "created",
                },
                {
                    "object": f"{database}.PUBLIC.whole_new_word(base string default 'word', "
                    "mult int default 2, suffix string default '!')",
                    "type": "function",
                    "status": "created",
                },
                {
                    "object": f"{database}.PUBLIC.check_all_types("
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

        _snowpark_test_steps.object_show_includes_given_identifiers(
            object_type="function",
            identifier=(
                "WHOLE_NEW_WORD",
                "(DEFAULT VARCHAR, DEFAULT NUMBER, DEFAULT VARCHAR) RETURN VARCHAR",
            ),
        )
        _snowpark_test_steps.object_show_includes_given_identifiers(
            object_type="procedure",
            identifier=(
                "WHOLE_NEW_WORD_PROCEDURE",
                "(DEFAULT VARCHAR, DEFAULT NUMBER, DEFAULT VARCHAR) RETURN VARCHAR",
            ),
        )

        # Created objects can be described
        _snowpark_test_steps.object_describe_should_return_entity_description(
            object_type="function",
            identifier="WHOLE_NEW_WORD(VARCHAR, NUMBER, VARCHAR)",
            signature="(BASE VARCHAR, MULT NUMBER, SUFFIX VARCHAR)",
            returns=RETURN_TYPE,
        )
        _snowpark_test_steps.object_describe_should_return_entity_description(
            object_type="procedure",
            identifier="WHOLE_NEW_WORD_PROCEDURE(VARCHAR, NUMBER, VARCHAR)",
            signature="(BASE VARCHAR, MULT NUMBER, SUFFIX VARCHAR)",
            returns=RETURN_TYPE,
        )

        # execute with default arguments
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="whole_new_word()",
            expected_value="wordword!",
        )
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="whole_new_word_procedure()",
            expected_value="wordword, but a procedure",
        )

        # execute naming arguments
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="whole_new_word(mult => 4, base => 'nii')",
            expected_value="niiniiniinii!",
        )
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="whole_new_word_procedure(mult => 4, base => 'nii')",
            expected_value="niiniiniinii, but a procedure",
        )

        # check default values for all types
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="check_all_types()",
            expected_value="s:<str>, i:7, b1:True, b2:True, f:1.5, l:[1, 2, 3]",
        )


@pytest.mark.integration
def test_snowpark_fully_qualified_name_v1(
    _snowpark_test_steps,
    runner,
    test_database,
    project_directory,
    alter_snowflake_yml,
):
    database = test_database.upper()
    default_schema = "PUBLIC"
    different_schema = "TOTALLY_DIFFERENT_SCHEMA"

    runner.invoke_with_connection(
        ["sql", "-q", f"create schema {database}.{different_schema}"]
    )
    with project_directory("snowpark_fully_qualified_name_v1") as tmp_dir:
        _snowpark_test_steps.snowpark_build_should_zip_files(
            additional_files=[Path("app.zip")]
        )

        # "default" database and schema provided by fully qualified name
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.0.name",
            value=f"{database}.{default_schema}.fqn_function",
        )
        # changed schema provided by fully qualified name
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.1.name",
            value=f"{database}.{different_schema}.fqn_function2",
        )
        # changed schema provided as argument
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.2.schema",
            value=different_schema,
        )
        # default database provided as argument
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.3.database",
            value=database,
        )
        # provide default database and changed schema as arguments
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.4.schema",
            value=different_schema,
        )
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="snowpark.functions.4.database",
            value=database,
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.{default_schema}.fqn_function(name string)",
                    "status": "created",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.fqn_function2(name string)",
                    "status": "created",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.schema_function(name "
                    "string)",
                    "status": "created",
                    "type": "function",
                },
                {
                    "object": f"{database}.{default_schema}.database_function(name string)",
                    "status": "created",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.database_schema_function(name "
                    "string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.{default_schema}.fqn_function(name string)",
                    "status": "packages updated",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.fqn_function2(name string)",
                    "status": "packages updated",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.schema_function(name "
                    "string)",
                    "status": "packages updated",
                    "type": "function",
                },
                {
                    "object": f"{database}.{default_schema}.database_function(name string)",
                    "status": "packages updated",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.database_schema_function(name "
                    "string)",
                    "status": "packages updated",
                    "type": "function",
                },
            ],
            additional_arguments=["--replace"],
        )


@pytest.mark.integration
def test_snowpark_fully_qualified_name_v2(
    _snowpark_test_steps,
    runner,
    test_database,
    project_directory,
    alter_snowflake_yml,
):
    database = test_database.upper()
    default_schema = "PUBLIC"
    different_schema = "TOTALLY_DIFFERENT_SCHEMA"

    runner.invoke_with_connection(
        ["sql", "-q", f"create schema {database}.{different_schema}"]
    )
    with project_directory("snowpark_fully_qualified_name_v2") as tmp_dir:
        _snowpark_test_steps.snowpark_build_should_zip_files(
            additional_files=[Path("app.zip")]
        )

        # "default" database and schema provided by fully qualified name
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="entities.fqn_function.identifier",
            value=f"{database}.{default_schema}.fqn_function",
        )
        # changed schema provided by fully qualified name
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="entities.fqn_function2.identifier",
            value=f"{database}.{different_schema}.fqn_function2",
        )
        # changed schema provided as argument
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="entities.schema_function.identifier.schema",
            value=different_schema,
        )
        # default database provided as argument
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="entities.database_function.identifier.database",
            value=database,
        )
        # procvide default database and changed schema as arguments
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="entities.database_schema_function.identifier.schema",
            value=different_schema,
        )
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="entities.database_schema_function.identifier.database",
            value=database,
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.{default_schema}.database_function(name string)",
                    "status": "created",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.database_schema_function(name string)",
                    "status": "created",
                    "type": "function",
                },
                {
                    "object": f"{database}.{default_schema}.fqn_function(name "
                    "string)",
                    "status": "created",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.fqn_function2(name string)",
                    "status": "created",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.schema_function(name "
                    "string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.{default_schema}.database_function(name string)",
                    "status": "packages updated",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.database_schema_function(name string)",
                    "status": "packages updated",
                    "type": "function",
                },
                {
                    "object": f"{database}.{default_schema}.fqn_function(name "
                    "string)",
                    "status": "packages updated",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.fqn_function2(name string)",
                    "status": "packages updated",
                    "type": "function",
                },
                {
                    "object": f"{database}.{different_schema}.schema_function(name "
                    "string)",
                    "status": "packages updated",
                    "type": "function",
                },
            ],
            additional_arguments=["--replace"],
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "project_name", ["snowpark_vectorized_v1", "snowpark_vectorized_v2"]
)
def test_snowpark_vector_function(
    _snowpark_test_steps,
    project_directory,
    alter_snowflake_yml,
    test_database,
    snowflake_session,
    project_name,
):
    database = test_database.upper()
    with project_directory(project_name):
        _snowpark_test_steps.snowpark_build_should_zip_files(
            additional_files=[Path("app.zip")]
        )

        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.PUBLIC.vector_func(x number(10, 5), y number(10, 5))",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        result = snowflake_session.execute_string(
            dedent(
                f"""
            select {database}.PUBLIC.VECTOR_FUNC(x, y)
            from (
              select 1 as x, 3.14::float as y union all
              select 2, 1.59 union all
              select 3, -0.5
            );
        """
            )
        )
        assert [r for r in result[0]] == [(4.14,), (3.59,), (2.5,)]


@pytest.mark.integration
def test_build_skip_version_check(
    runner, project_directory, alter_requirements_txt, test_database
):
    # test case: package is available in Anaconda, but not in required version
    with project_directory("snowpark") as tmp_dir:
        alter_requirements_txt(tmp_dir / "requirements.txt", ["matplotlib>=1000"])
        result = runner.invoke_with_connection(["snowpark", "build"])
        assert result.exit_code == 1, result.output
        assert "Error" in result.output
        assert (
            "pip wheel finished with error code 1. Please re-run with --verbose or"
            in result.output
        )
        assert "--debug for more details." in result.output

        result = runner.invoke_with_connection(
            ["snowpark", "build", "--skip-version-check"]
        )
        assert result.exit_code == 0, result.output
        assert "Build done." in result.output
        assert "Creating dependencies.zip" not in result.output
        assert "Creating: app.zip" in result.output


@pytest.mark.integration
@pytest.mark.skipif(sys.version_info >= (3, 12), reason="Unknown issues")
@pytest.mark.parametrize(
    "flags",
    [
        ["--allow-shared-libraries"],
        ["--allow-shared-libraries", "--ignore-anaconda"],
    ],
)
def test_build_with_anaconda_dependencies(
    flags, runner, project_directory, alter_requirements_txt, test_database
):
    with project_directory("snowpark") as tmp_dir:
        alter_requirements_txt(tmp_dir / "requirements.txt", ["july", "snowflake.core"])
        result = runner.invoke_with_connection(["snowpark", "build", *flags])
        assert result.exit_code == 0, result.output
        assert "Build done." in result.output
        assert "Creating dependencies.zip" in result.output
        assert "Creating: app.zip" in result.output

        requirements_snowflake = tmp_dir / "requirements.snowflake.txt"
        if "--ignore-anaconda" in flags:
            assert not requirements_snowflake.exists()
        else:
            assert requirements_snowflake.exists()
            assert "matplotlib" in requirements_snowflake.read_text()
            assert "numpy" in requirements_snowflake.read_text()
            assert "snowflake.core" in requirements_snowflake.read_text()


@pytest.mark.integration
def test_build_with_non_anaconda_dependencies(
    runner, project_directory, alter_requirements_txt, test_database
):
    with project_directory("snowpark") as tmp_dir:
        alter_requirements_txt(
            tmp_dir / "requirements.txt", ["dummy-pkg-for-tests-with-deps"]
        )
        result = runner.invoke_with_connection(["snowpark", "build"])
        assert result.exit_code == 0, result.output
        assert "Build done." in result.output
        assert "Creating dependencies.zip" in result.output
        assert "Creating: app.zip" in result.output

        files = ZipFile(tmp_dir / "dependencies.zip").namelist()
        assert "dummy_pkg_for_tests/shrubbery.py" in files
        assert "dummy_pkg_for_tests_with_deps/shrubbery.py" in files


@pytest.mark.integration
@pytest.mark.skipif(sys.version_info >= (3, 12), reason="Unknown issues")
def test_build_shared_libraries_error(
    runner, project_directory, alter_requirements_txt, test_database
):
    with project_directory("snowpark") as tmp_dir:
        alter_requirements_txt(tmp_dir / "requirements.txt", ["numpy"])
        result = runner.invoke_with_connection(
            ["snowpark", "build", "--ignore-anaconda"]
        )
        assert result.exit_code == 1, result.output
        assert (
            "Some packages contain shared (.so/.dll) libraries. Try again with"
            in result.output
        )
        assert "--allow-shared-libraries." in result.output
        assert "Build done." not in result.output


@pytest.mark.integration
def test_dependency_search_optimization(
    runner, project_directory, alter_requirements_txt, test_database
):
    with project_directory("snowpark") as tmp_dir:
        alter_requirements_txt(tmp_dir / "requirements.txt", ["july"])
        result = runner.invoke_with_connection(["snowpark", "build"])
        assert result.exit_code == 0, result.output
        snowflake_deps_txt = tmp_dir / "requirements.snowflake.txt"
        assert snowflake_deps_txt.exists()
        deps = set(snowflake_deps_txt.read_text().splitlines())
        assert deps == {"matplotlib", "numpy", "snowflake-snowpark-python"}


@pytest.mark.integration
def test_build_package_from_github(
    runner, project_directory, alter_requirements_txt, test_database
):
    with project_directory("snowpark") as tmp_dir:
        alter_requirements_txt(
            tmp_dir / "requirements.txt",
            [
                "git+https://github.com/sfc-gh-turbaszek/dummy-pkg-for-tests-with-deps.git"
            ],
        )
        result = runner.invoke_with_connection(["snowpark", "build"])
        assert result.exit_code == 0, result.output
        assert "Build done." in result.output
        assert "Creating dependencies.zip" in result.output
        assert "Creating: app.zip" in result.output

        assert (
            "dummy_pkg_for_tests/shrubbery.py"
            in ZipFile(tmp_dir / "dependencies.zip").namelist()
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "flag, project_name",
    [
        ("--ignore-anaconda", "snowpark_version_check_v1"),
        ("--ignore-anaconda", "snowpark_version_check_v2"),
        ("", "snowpark_version_check_v1"),
        ("", "snowpark_version_check_v2"),
    ],
)
def test_ignore_anaconda_uses_version_from_zip(
    project_directory, runner, test_database, flag, project_name
):
    with project_directory(project_name):
        command = ["snowpark", "build", "--allow-shared-libraries"]
        if flag:
            command.append(flag)
        result = runner.invoke_with_connection(command)
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection(["snowpark", "deploy"])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            ["snowpark", "execute", "function", "check_mypy_version()"]
        )
        assert result.exit_code == 0, result.output
        # earliest mypy 1.* version is 1.5
        assert result.json == {"CHECK_MYPY_VERSION()": "1.3.0"}


@pytest.mark.integration
def test_incorrect_requirements(project_directory, runner, alter_requirements_txt):
    from packaging.requirements import InvalidRequirement

    with project_directory("snowpark") as tmp_dir:
        alter_requirements_txt(
            tmp_dir / "requirements.txt", ["this is incorrect requirement"]
        )
        with pytest.raises(InvalidRequirement) as err:
            runner.invoke_with_connection(["snowpark", "build"])
        assert (
            "Expected end or semicolon (after name and no valid version specifier)"
            in str(err)
        )


@pytest.mark.integration
def test_snowpark_aliases(
    project_directory, runner, _snowpark_test_steps, test_database
):
    with project_directory("snowpark"):
        for command in ["build", "deploy"]:
            result = runner.invoke_with_connection_json(["snowpark", command])
            assert result.exit_code == 0, result

        for command in [
            ["list", "function"],
            ["list", "procedure", "--like", "hello%"],
            ["describe", "function", "hello_function(string)"],
            ["describe", "procedure", "test()"],
        ]:
            expected_result = runner.invoke_with_connection_json(["object", *command])
            assert expected_result.exit_code == 0, expected_result.output
            result = runner.invoke_with_connection_json(["snowpark", *command])
            assert result.exit_code == 0, result
            assert result.json == expected_result.json

        result = runner.invoke_with_connection_json(
            ["snowpark", "drop", "function", "hello_function(string)"]
        )
        assert result.exit_code == 0, result.output
        assert result.json == [
            {
                "status": "HELLO_FUNCTION successfully dropped.",
            },
        ]

        result = runner.invoke_with_connection_json(
            ["snowpark", "drop", "procedure", "hello_procedure(string)"]
        )
        assert result.exit_code == 0, result.output
        assert result.json == [
            {
                "status": "HELLO_PROCEDURE successfully dropped.",
            },
        ]


@pytest.mark.integration
def test_snowpark_flow_v2(
    _snowpark_test_steps,
    project_directory,
    alter_snowflake_yml,
    test_database,
    enable_snowpark_glob_support_feature_flag,
):
    database = test_database.upper()
    with project_directory("snowpark_v2") as tmp_dir:
        _snowpark_test_steps.snowpark_build_should_zip_files(
            additional_files=[
                Path("output"),
                Path("output") / "bundle",
                bundle_root,
                bundle_root / "app_1.zip",
                bundle_root / "app_2.zip",
                bundle_root / "c.py",
            ]
        )
        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        _snowpark_test_steps.assert_those_procedures_are_in_snowflake(
            "hello_procedure(VARCHAR) RETURN VARCHAR"
        )
        _snowpark_test_steps.assert_those_functions_are_in_snowflake(
            "hello_function(VARCHAR) RETURN VARCHAR"
        )

        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            "stage_a/app_1.zip",
            "stage_a/dependencies.zip",
            stage_name="stage_a",
        )

        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"{STAGE_NAME}/app_2.zip",
            f"{STAGE_NAME}/c.py",
            f"{STAGE_NAME}/dependencies.zip",
            stage_name=STAGE_NAME,
        )

        # Created objects can be executed
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="hello_procedure('foo')",
            expected_value="Hello foo",
        )

        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="test()",
            expected_value="Test procedure",
        )

        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="hello_function('foo')",
            expected_value="Hello foo!",
        )


@pytest.mark.integration
def test_snowpark_flow_v2_old_build(
    _snowpark_test_steps, project_directory, alter_snowflake_yml, test_database
):
    database = test_database.upper()
    with project_directory("snowpark_v2") as tmp_dir:
        _snowpark_test_steps.snowpark_build_should_zip_files(
            additional_files=[Path("app_1.zip"), Path("app_2.zip")]
        )
        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )

        _snowpark_test_steps.assert_those_procedures_are_in_snowflake(
            "hello_procedure(VARCHAR) RETURN VARCHAR"
        )
        _snowpark_test_steps.assert_those_functions_are_in_snowflake(
            "hello_function(VARCHAR) RETURN VARCHAR"
        )

        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            "stage_a/app_1.zip",
            "stage_a/dependencies.zip",
            stage_name="stage_a",
        )

        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"{STAGE_NAME}/app_2.zip",
            f"{STAGE_NAME}/c.py",
            f"{STAGE_NAME}/dependencies.zip",
            stage_name=STAGE_NAME,
        )

        # Created objects can be executed
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="hello_procedure('foo')",
            expected_value="Hello foo",
        )

        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="test()",
            expected_value="Test procedure",
        )

        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="function",
            identifier="hello_function('foo')",
            expected_value="Hello foo!",
        )


@pytest.mark.integration
def test_snowpark_with_glob_patterns(
    _snowpark_test_steps,
    project_directory,
    alter_snowflake_yml,
    test_database,
    enable_snowpark_glob_support_feature_flag,
):
    database = test_database.upper()
    with project_directory("snowpark_glob_patterns"):
        _snowpark_test_steps.snowpark_build_should_zip_files(
            additional_files=[
                Path("output"),
                Path("output") / "bundle",
                bundle_root,
                bundle_root / "app_1.zip",
                bundle_root / "app_2.zip",
                bundle_root / "e.py",
            ]
        )
        _snowpark_test_steps.snowpark_deploy_should_finish_successfully_and_return(
            [
                {
                    "object": f"{database}.PUBLIC.hello_procedure(name string)",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.test()",
                    "status": "created",
                    "type": "procedure",
                },
                {
                    "object": f"{database}.PUBLIC.hello_function(name string)",
                    "status": "created",
                    "type": "function",
                },
            ]
        )
        _snowpark_test_steps.snowpark_execute_should_return_expected_value(
            object_type="procedure",
            identifier="hello_procedure('foo')",
            expected_value="Hello foo" + "Test procedure",
        )


@pytest.mark.integration
def test_snowpark_deploy_prune_flag(
    project_directory, test_database, runner, _snowpark_test_steps
):
    with project_directory("snowpark_v2") as project_root:
        # upload unexpected file to stage
        unexpected_file = project_root / "unexpected.txt"
        unexpected_file.write_text("This is unexpected.")
        result = runner.invoke_with_connection(["stage", "create", STAGE_NAME])
        assert result.exit_code == 0, result.output
        runner.invoke_with_connection(
            ["stage", "copy", str(unexpected_file), f"@{STAGE_NAME}"]
        )
        assert result.exit_code == 0, result.output

        # build snowpark
        result = runner.invoke_with_connection(["snowpark", "build"])
        assert result.exit_code == 0, result.output

        # deploy without "prune" should not remove file from stage
        result = runner.invoke_with_connection(["snowpark", "deploy"])
        assert result.exit_code == 0, result.output
        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"{STAGE_NAME}/app_2.zip",
            f"{STAGE_NAME}/c.py",
            f"{STAGE_NAME}/dependencies.zip",
            f"{STAGE_NAME}/unexpected.txt",
            stage_name=STAGE_NAME,
        )

        # deploy with "prune" should remove file from stage
        result = runner.invoke_with_connection(
            ["snowpark", "deploy", "--replace", "--prune"]
        )
        assert result.exit_code == 0, result.output
        _snowpark_test_steps.assert_that_only_these_files_are_staged_in_test_db(
            f"{STAGE_NAME}/app_2.zip",
            f"{STAGE_NAME}/c.py",
            f"{STAGE_NAME}/dependencies.zip",
            stage_name=STAGE_NAME,
        )


@pytest.mark.integration
def test_if_excluding_version_of_anaconda_package_moves_it_to_other_requirements(
    runner, project_directory, alter_requirements_txt
):
    """
    This test checks if specifying version of package, aviailable in Anaconda, using '!=' results in resolving it as a
    non-anaconda package.
    This is  a workaround for an issue with missing operator in Snowpark.
    Package 'about-time' is used in tests for two reasons: it is available in Anaconda and it is relatively small (13kb for a wheel).
    """
    with project_directory("snowpark_v2") as tmp_dir:
        alter_requirements_txt(tmp_dir / "requirements.txt", ["about-time!=3.1.1"])
        result = runner.invoke_with_connection_json(["snowpark", "build"])
        assert result.exit_code == 0, result.output
        assert Path(tmp_dir / "dependencies.zip").is_file()

        with open("requirements.snowflake.txt") as f:
            assert "about-time!=3.1.1" not in f.read()

        with ZipFile("dependencies.zip") as zf:
            assert any("about_time" in name for name in zf.namelist())


@pytest.mark.integration
def test_using_external_packages_from_package_repository(
    test_database, runner, project_directory, alter_snowflake_yml
):

    with project_directory("snowpark_artifact_repository") as tmp_dir:
        result = runner.invoke_with_connection(
            [
                "snowpark",
                "build",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Build done." in result.output

        result = runner.invoke_with_connection(
            [
                "snowpark",
                "deploy",
            ]
        )

        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection(
            [
                "snowpark",
                "execute",
                "function",
                "test_function()",
                "--warehouse",
                "snowpark_tests",
            ]
        )

        assert result.exit_code == 0, result.output
        assert "We want... a shrubbery!" in result.output

        result = runner.invoke_with_connection(
            [
                "snowpark",
                "execute",
                "procedure",
                "test_procedure()",
                "--warehouse",
                "snowpark_tests",
            ]
        )

        assert result.exit_code == 0, result.output
        assert "We want... a shrubbery!" in result.output

        # Update packages
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="mixins.snowpark_shared.artifact_repository_packages",
            value=["dummy-pkg-for-tests", "dummy-pkg-for-tests-with-deps"],
        )

        result = runner.invoke_with_connection_json(
            [
                "snowpark",
                "deploy",
                "--replace",
            ]
        )

        assert result.exit_code == 0, result.output
        assert all(
            entity.get("status", "") == "definition updated" for entity in result.json
        )

        # TODO: after introducing more available repositories, add repository change case

        # Update constraints
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            parameter_path="mixins.snowpark_shared.resource_constraint",
            value={"architecture": "x86"},
        )
        result = runner.invoke_with_connection_json(
            ["snowpark", "deploy", "--replace", "--warehouse", "snowpark_tests"]
        )

        assert result.exit_code == 0, result.output
        assert all(
            entity.get("status", "") == "definition updated" for entity in result.json
        )


@pytest.fixture
def _test_setup(
    runner,
    sql_test_helper,
    test_database,
    temporary_working_directory,
    snapshot,
):
    snowpark_procedure_test_setup = FlowTestSetup(
        runner=runner,
        sql_test_helper=sql_test_helper,
        test_database=test_database,
        snapshot=snapshot,
    )
    yield snowpark_procedure_test_setup


@pytest.fixture
def _snowpark_test_steps(_test_setup):
    yield SnowparkTestSteps(_test_setup)


@pytest.fixture
def alter_requirements_txt():
    def update(requirements_path: Path, requirements: List[str]):
        requirements.append("snowflake-snowpark-python")
        requirements_path.write_text("\n".join(requirements))

    yield update
