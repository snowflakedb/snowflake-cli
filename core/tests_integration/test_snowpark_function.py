from __future__ import annotations

import re
from typing import Any, List, Dict

import pytest
from snowflake.connector import SnowflakeConnection
from syrupy import SnapshotAssertion

from tests_integration.testing_utils.file_utils import replace_text_in_file
from tests_integration.conftest import runner
from tests_integration.snowflake_connector import test_database, snowflake_session
from tests_integration.testing_utils.sql_utils import sql_test_helper
from tests_integration.testing_utils.naming_utils import object_name_provider
from tests_integration.testing_utils.working_directory_utils import (
    temporary_working_directory,
)

from tests_integration.conftest import SnowCLIRunner
from tests_integration.testing_utils.assertions.test_file_assertions import (
    assert_that_current_working_directory_contains_only_following_files,
    assert_that_file_content_is_equal_to_snapshot,
)
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful_and_has_no_output,
    assert_that_result_is_successful_and_output_contains,
    assert_that_result_is_successful,
    assert_that_result_is_successful_and_json_output_contains_value_at_path,
)
from tests_integration.testing_utils.naming_utils import ObjectNameProvider
from tests_integration.testing_utils.sql_utils import SqlTestHelper


@pytest.mark.integration
def test_snowpark_function_flow(_test_steps):
    _test_steps.assert_that_no_functions_are_in_snowflake()
    _test_steps.assert_that_no_files_are_staged_in_snowflake()

    _test_steps.snowpark_function_list_should_return_no_data()

    _test_steps.snowpark_function_init_should_initialize_files_with_default_content()
    _test_steps.snowpark_function_package_should_zip_files()

    function_name = _test_steps.snowpark_function_create_should_finish_successfully()
    _test_steps.assert_that_only_these_functions_are_in_snowflake(
        f"{function_name}() RETURN VARCHAR"
    )
    _test_steps.assert_that_only_these_files_are_staged_in_snowflake(
        f"deployments/{function_name}/app.zip"
    )

    _test_steps.snowpark_function_list_should_return_list_containing_function_at_the_first_place(
        function_name=function_name,
        arguments="()",
        result_type="VARCHAR",
    )

    _test_steps.snowpark_function_describe_should_return_function_description(
        function_name=function_name,
        arguments="()",
    )

    _test_steps.snowpark_function_execute_should_return_expected_value(
        function_name=function_name,
        arguments="()",
        expected_value="Hello World!",
    )

    _test_steps.snowpark_function_update_should_finish_successfully(function_name)
    _test_steps.assert_that_only_these_functions_are_in_snowflake(
        f"{function_name}() RETURN NUMBER"
    )
    _test_steps.assert_that_only_these_files_are_staged_in_snowflake(
        f"deployments/{function_name}/app.zip"
    )

    _test_steps.snowpark_function_list_should_return_list_containing_function_at_the_first_place(
        function_name=function_name,
        arguments="()",
        result_type="NUMBER",
    )

    _test_steps.snowpark_function_execute_should_return_expected_value(
        function_name=function_name,
        arguments="()",
        expected_value=1,
    )

    _test_steps.snowpark_function_drop_should_finish_successfully(
        function_name=function_name,
        arguments="()",
    )
    _test_steps.assert_that_no_functions_are_in_snowflake()
    _test_steps.assert_that_only_these_files_are_staged_in_snowflake(
        f"deployments/{function_name}/app.zip"
    )

    _test_steps.snowpark_function_list_should_return_no_data()


@pytest.fixture
def _test_setup(
    runner,
    snowflake_session,
    test_database,
    sql_test_helper,
    object_name_provider,
    temporary_working_directory,
    snapshot,
):
    snowpark_function_test_setup = SnowparkFunctionTestSetup(
        runner=runner,
        snowflake_session=snowflake_session,
        sql_test_helper=sql_test_helper,
        object_name_provider=object_name_provider,
        snapshot=snapshot,
    )
    yield snowpark_function_test_setup
    snowpark_function_test_setup.clean_after_test_case()


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkFunctionTestSteps(_test_setup)


class SnowparkFunctionTestSetup:
    def __init__(
        self,
        runner: SnowCLIRunner,
        snowflake_session: SnowflakeConnection,
        sql_test_helper: SqlTestHelper,
        object_name_provider: ObjectNameProvider,
        snapshot: SnapshotAssertion,
    ):
        self.runner = runner
        self.snowflake_session = snowflake_session
        self.sql_test_helper = sql_test_helper
        self.test_object_name_provider = object_name_provider
        self.object_name_prefix = object_name_provider.get_object_name_prefix()
        self.snapshot = snapshot

    def query_functions_created_in_this_test_case(self) -> List[Dict[str, Any]]:
        results = self.sql_test_helper.execute_single_sql(
            f"SHOW USER FUNCTIONS LIKE '{self.object_name_prefix.upper()}%'"
        )
        return results

    def query_stages_in_database(self) -> List[Dict[str, Any]]:
        return self.sql_test_helper.execute_single_sql(
            f"SHOW STAGES IN DATABASE {self.snowflake_session.database}"
        )

    def query_files_uploaded_in_this_test_case(self) -> List[Dict[str, Any]]:
        stages_in_database = [
            stage["name"].lower() for stage in self.query_stages_in_database()
        ]
        if "deployments" in stages_in_database:
            all_staged_files = self.sql_test_helper.execute_single_sql(
                f"LIST @deployments"
            )
            filed_uploaded_in_this_test_case = [
                staged_file
                for staged_file in all_staged_files
                if staged_file["name"]
                .lower()
                .startswith(f"deployments/{self.object_name_prefix.lower()}")
            ]
            return filed_uploaded_in_this_test_case
        else:
            return []

    def clean_after_test_case(self) -> None:
        self.delete_functions_created_in_this_test_case()
        self.delete_files_uploaded_in_this_test_case()

    def delete_functions_created_in_this_test_case(self) -> None:
        functions_to_delete = self.query_functions_created_in_this_test_case()
        for function in functions_to_delete:
            name_with_args = re.sub(r" RETURN .*", "", function["arguments"])
            self.sql_test_helper.execute_single_sql(f"DROP FUNCTION {name_with_args}")

    def delete_files_uploaded_in_this_test_case(self) -> None:
        files_to_remove = self.query_files_uploaded_in_this_test_case()
        for file_to_remove in files_to_remove:
            file_path_at_stage = file_to_remove["name"]
            self.sql_test_helper.execute_single_sql(f"REMOVE @{file_path_at_stage}")


class SnowparkFunctionTestSteps:
    def __init__(self, setup: SnowparkFunctionTestSetup):
        self._setup = setup

    def snowpark_function_list_should_return_no_data(self) -> None:
        result = self._setup.runner.invoke_with_config_and_integration_connection(
            [
                "snowpark",
                "function",
                "list",
                "--like",
                f"{self._setup.object_name_prefix}%",
            ]
        )
        assert_that_result_is_successful_and_output_contains(result, "No data")

    def snowpark_function_list_should_return_list_containing_function_at_the_first_place(
        self, function_name: str, arguments: str, result_type: str
    ) -> None:
        result = self._setup.runner.invoke_with_config_and_integration_connection(
            [
                "--format",
                "json",
                "snowpark",
                "function",
                "list",
                "--like",
                f"{self._setup.object_name_prefix}%",
            ]
        )
        assert_that_result_is_successful_and_json_output_contains_value_at_path(
            result=result, path=[0, "name"], expected_value=function_name.upper()
        )
        assert_that_result_is_successful_and_json_output_contains_value_at_path(
            result=result,
            path=[0, "arguments"],
            expected_value=f"{function_name}{arguments} RETURN {result_type}".upper(),
        )
        assert_that_result_is_successful_and_json_output_contains_value_at_path(
            result=result, path=[0, "language"], expected_value="PYTHON"
        )

    def snowpark_function_execute_should_return_expected_value(
        self, function_name: str, arguments: str, expected_value: Any
    ) -> None:
        function_with_arguments = f"{function_name}{arguments}"
        result = self._setup.runner.invoke_with_config_and_integration_connection(
            [
                "--format",
                "json",
                "snowpark",
                "function",
                "execute",
                "--function",
                function_with_arguments,
            ]
        )
        assert_that_result_is_successful_and_json_output_contains_value_at_path(
            result=result,
            path=[0, function_with_arguments.upper()],
            expected_value=expected_value,
        )

    def snowpark_function_describe_should_return_function_description(
        self, function_name: str, arguments: str
    ) -> None:
        result = self._setup.runner.invoke_with_config_and_integration_connection(
            [
                "--format",
                "json",
                "snowpark",
                "function",
                "describe",
                "--name",
                function_name,
                "--input-parameters",
                arguments,
            ]
        )
        assert_that_result_is_successful_and_json_output_contains_value_at_path(
            result=result,
            path=[0, "property"],
            expected_value="signature",
        )
        assert_that_result_is_successful_and_json_output_contains_value_at_path(
            result=result,
            path=[0, "value"],
            expected_value=arguments,
        )
        assert_that_result_is_successful_and_output_contains(
            result=result, expected_output=function_name, ignore_case=True
        )

    def snowpark_function_init_should_initialize_files_with_default_content(
        self,
    ) -> None:
        result = self._setup.runner.invoke_with_config(["snowpark", "function", "init"])
        assert_that_result_is_successful_and_has_no_output(result)
        assert_that_current_working_directory_contains_only_following_files(
            ".gitignore", "app.py", "config.toml", "requirements.txt"
        )
        assert_that_file_content_is_equal_to_snapshot(
            actual_file_path="app.py",
            snapshot=self._setup.snapshot(name="app.py"),
        )
        assert_that_file_content_is_equal_to_snapshot(
            actual_file_path="config.toml",
            snapshot=self._setup.snapshot(name="config.toml"),
        )
        assert_that_file_content_is_equal_to_snapshot(
            actual_file_path="requirements.txt",
            snapshot=self._setup.snapshot(name="requirements.txt"),
        )
        assert_that_file_content_is_equal_to_snapshot(
            actual_file_path=".gitignore",
            snapshot=self._setup.snapshot(name="gitignore"),
        )

    def snowpark_function_package_should_zip_files(self) -> None:
        result = self._setup.runner.invoke_with_config(
            ["snowpark", "function", "package"]
        )
        assert_that_result_is_successful_and_has_no_output(result)
        assert_that_current_working_directory_contains_only_following_files(
            ".gitignore",
            "app.py",
            "config.toml",
            "requirements.snowflake.txt",
            "requirements.txt",
            "app.zip",
        )

    def snowpark_function_create_should_finish_successfully(
        self,
    ) -> str:
        function_name = (
            self._setup.test_object_name_provider.create_and_get_next_object_name()
        )
        result = self._setup.runner.invoke_with_config_and_integration_connection(
            [
                "snowpark",
                "function",
                "create",
                "--name",
                function_name,
                "--handler",
                "app.hello",
                "--input-parameters",
                "()",
                "--return-type",
                "string",
            ]
        )
        assert_that_result_is_successful(result)
        return function_name

    def snowpark_function_update_should_finish_successfully(
        self,
        function_name: str,
    ) -> None:
        replace_text_in_file(
            file_path="app.py",
            to_replace="def hello() -> str:",
            replacement="def hello() -> int:",
        )
        replace_text_in_file(
            file_path="app.py",
            to_replace='return "Hello World!"',
            replacement="return 1",
        )
        result = self._setup.runner.invoke_with_config_and_integration_connection(
            [
                "snowpark",
                "function",
                "update",
                "--name",
                function_name,
                "--handler",
                "app.hello",
                "--input-parameters",
                "()",
                "--return-type",
                "int",
            ]
        )
        assert_that_result_is_successful(result)

    def snowpark_function_drop_should_finish_successfully(
        self,
        function_name: str,
        arguments: str,
    ) -> None:
        result = self._setup.runner.invoke_with_config_and_integration_connection(
            [
                "snowpark",
                "function",
                "drop",
                "--name",
                function_name,
                "--input-parameters",
                arguments,
            ]
        )
        assert_that_result_is_successful(result)

    def assert_that_no_functions_are_in_snowflake(self) -> None:
        self.assert_that_only_these_functions_are_in_snowflake()

    def assert_that_only_these_functions_are_in_snowflake(
        self, *expected_full_function_signatures: str
    ) -> None:
        actual_function_signatures = [
            function["arguments"]
            for function in self._setup.query_functions_created_in_this_test_case()
        ]
        adjusted_expected_full_function_signatures = [
            function.upper() for function in expected_full_function_signatures
        ]
        assert set(actual_function_signatures) == set(
            adjusted_expected_full_function_signatures
        )

    def assert_that_no_files_are_staged_in_snowflake(self) -> None:
        self.assert_that_only_these_files_are_staged_in_snowflake()

    def assert_that_only_these_files_are_staged_in_snowflake(
        self, *expected_file_paths: str
    ) -> None:
        actual_file_paths = [
            staged_file["name"]
            for staged_file in self._setup.query_files_uploaded_in_this_test_case()
        ]
        assert set(actual_file_paths) == set(expected_file_paths)
