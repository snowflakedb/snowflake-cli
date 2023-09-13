from __future__ import annotations

import json
import os
import re
from enum import Enum
from syrupy import SnapshotAssertion
from typing import List, Dict, Any

from snowflake.connector import SnowflakeConnection

from tests_integration.test_utils import contains_row_with
from tests_integration.conftest import SnowCLIRunner
from tests_integration.testing_utils.assertions.test_file_assertions import (
    assert_that_file_content_is_equal_to_snapshot,
    assert_that_current_working_directory_contains_only_following_files,
)
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_contains_row_with,
    assert_that_result_is_successful_and_done_is_on_output,
    assert_that_result_is_successful_and_output_json_equals,
    assert_that_result_is_successful,
)
from tests_integration.testing_utils.file_utils import replace_text_in_file
from tests_integration.testing_utils.naming_utils import ObjectNameProvider
from tests_integration.testing_utils.sql_utils import SqlTestHelper

# TODO name 'entity' for both function and procedure does not seem to be perfect.
#  But i can`t come up with anything better


class TestType(Enum):
    FUNCTION = "function"
    PROCEDURE = "procedure"


class SnowparkTestSetup:
    def __init__(
        self,
        runner: SnowCLIRunner,
        snowflake_session: SnowflakeConnection,
        sql_test_helper: SqlTestHelper,
        object_name_provider: ObjectNameProvider,
        test_database,
        test_type: TestType,
        snapshot: SnapshotAssertion,
    ):
        self.runner = runner
        self.snowflake_session = snowflake_session
        self.sql_test_helper = sql_test_helper
        self.test_object_name_provider = object_name_provider
        self.object_name_prefix = object_name_provider.get_object_name_prefix()
        self.test_type = test_type
        self.snapshot = snapshot

    def query_entities_created_in_this_test_case(self) -> List[Dict[str, Any]]:
        if self.test_type == TestType.FUNCTION:
            query_string = (
                f"SHOW USER FUNCTIONS LIKE '{self.object_name_prefix.upper()}%'"
            )
        else:
            query_string = f"SHOW PROCEDURES LIKE '{self.object_name_prefix.upper()}%'"

        return self.sql_test_helper.execute_single_sql(query_string)

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
        self.delete_entities_created_in_this_test_case()
        self.delete_files_uploaded_in_this_test_case()

    def delete_entities_created_in_this_test_case(self) -> None:
        entities_to_delete = self.query_entities_created_in_this_test_case()
        for obj in entities_to_delete:
            name_with_args = re.sub(r" RETURN .*", "", obj["arguments"])
            self.sql_test_helper.execute_single_sql(
                f"DROP {self.test_type.value.upper()} {name_with_args}"
            )

    def delete_files_uploaded_in_this_test_case(self) -> None:
        files_to_remove = self.query_files_uploaded_in_this_test_case()
        for file_to_remove in files_to_remove:
            file_path_at_stage = file_to_remove["name"]
            self.sql_test_helper.execute_single_sql(f"REMOVE @{file_path_at_stage}")


class SnowparkTestSteps:
    dir_contents = {
        "function": [".gitignore", "app.py", "config.toml", "requirements.txt"],
        "procedure": [
            "requirements.txt",
            "local_connection.py",
            ".gitignore",
            "app.py",
            "config.toml",
        ],
    }

    def __init__(self, setup: SnowparkTestSetup, test_type: TestType):
        self._setup = setup
        self.test_type = test_type

    def snowpark_list_should_return_no_data(self) -> None:
        result = self._setup.runner.invoke_integration(
            [
                "snowpark",
                self.test_type.value,
                "list",
                "--like",
                f"{self._setup.object_name_prefix}%",
            ]
        )
        assert_that_result_is_successful_and_output_json_equals(result, [])

    def snowpark_list_should_return_entity_at_first_place(
        self, entity_name: str, arguments: str, result_type: str
    ) -> None:
        result = self._setup.runner.invoke_integration(
            [
                "snowpark",
                self.test_type.value,
                "list",
                "--like",
                f"{self._setup.object_name_prefix}%",
            ]
        )
        assert_that_result_is_successful(result)
        assert_that_result_contains_row_with(
            result,
            {
                "name": entity_name.upper(),
                "arguments": f"{entity_name}{arguments} RETURN {result_type}".upper(),
            },
        )

        if self.test_type == TestType.FUNCTION:
            assert_that_result_contains_row_with(result, {"language": "PYTHON"})

    def snowpark_execute_should_return_expected_value(
        self, entity_name: str, arguments: str, expected_value: Any
    ) -> None:
        name_with_arguments = f"{entity_name}{arguments}"
        result = self._setup.runner.invoke_integration(
            [
                "snowpark",
                self.test_type.value,
                "execute",
                f"--{self.test_type.value}",
                name_with_arguments,
            ]
        )

        assert_that_result_is_successful(result)
        assert_that_result_contains_row_with(
            result,
            {
                name_with_arguments.upper()
                if self.test_type == TestType.FUNCTION
                else entity_name.upper(): expected_value
            },
        )

    def snowpark_describe_should_return_entity_description(
        self, entity_name: str, arguments: str
    ) -> None:
        result = self._setup.runner.invoke_integration(
            [
                "snowpark",
                self.test_type.value,
                "describe",
                "--name",
                entity_name,
                "--input-parameters",
                arguments,
            ]
        )
        assert_that_result_is_successful(result)
        assert_that_result_contains_row_with(
            result, {"property": "signature", "value": arguments}
        )
        assert result.json is not None

    def snowpark_init_should_initialize_files_with_default_content(
        self,
    ) -> None:
        result = self._setup.runner.invoke_with_config(
            ["snowpark", self.test_type.value, "init", "--format", "JSON"]
        )
        file_list = self.dir_contents[self.test_type.value]
        assert_that_result_is_successful_and_done_is_on_output(result)
        assert_that_current_working_directory_contains_only_following_files(*file_list)

        for file in file_list:
            assert_that_file_content_is_equal_to_snapshot(
                actual_file_path=file, snapshot=self._setup.snapshot(name=file)
            )

    def requirements_file_should_contain_coverage(self, file_name="requirements.txt"):
        assert os.path.exists(file_name)

        with open(file_name, "r") as req_file:
            file_contents = req_file.readlines()
            assert file_contents
            assert "coverage\n" in file_contents

    def snowpark_package_should_zip_files(self) -> None:
        result = self._setup.runner.invoke_with_config(
            [
                "snowpark",
                self.test_type.value,
                "package",
                "--pypi-download",
                "yes",
                "--format",
                "JSON",
            ]
        )
        assert_that_result_is_successful_and_done_is_on_output(result)
        assert_that_current_working_directory_contains_only_following_files(
            *self.dir_contents[self.test_type.value],
            "app.zip",
            "requirements.snowflake.txt",
        )

    def snowpark_create_should_finish_successfully(self) -> str:
        return self.run_create()

    def snowpark_create_with_coverage_wrapper_should_finish_succesfully(self) -> str:
        return self.run_create("--install-coverage-wrapper")

    def run_create(self, additional_arguments=""):
        entity_name = (
            self._setup.test_object_name_provider.create_and_get_next_object_name()
        )

        arguments = [
            "snowpark",
            self.test_type.value,
            "create",
            "--name",
            entity_name,
            "--handler",
            "app.hello",
            "--input-parameters",
            "()",
            "--return-type",
            "string",
        ]

        if additional_arguments:
            arguments.append(additional_arguments)

        result = self._setup.runner.invoke_integration(arguments)

        assert_that_result_is_successful(result)
        return entity_name

    def snowpark_update_should_not_replace_if_the_signature_does_not_change(
        self, entity_name: str
    ):
        replace_text_in_file(
            file_path="app.py",
            to_replace='return "Hello World!"',
            replacement='return "Hello Snowflakes!"',
        )

        result = self._setup.runner.invoke_integration(
            [
                "snowpark",
                self.test_type.value,
                "update",
                "--name",
                entity_name,
                "--handler",
                "app.hello",
                "--input-parameters",
                "()",
                "--return-type",
                "string",
            ]
        )

        assert_that_result_is_successful_and_output_json_equals(
            result, {"message": "No packages to update. Deployment complete!"}
        )

    def snowpark_update_should_finish_successfully(
        self,
        entity_name: str,
    ) -> None:
        replace_text_in_file(
            file_path="app.py",
            to_replace="def hello() -> str:",
            replacement="def hello() -> int:",
        )
        replace_text_in_file(
            file_path="app.py",
            to_replace='return "Hello Snowflakes!"',
            replacement="return 1",
        )
        result = self._setup.runner.invoke_integration(
            [
                "snowpark",
                self.test_type.value,
                "update",
                "--name",
                entity_name,
                "--handler",
                "app.hello",
                "--input-parameters",
                "()",
                "--return-type",
                "int",
            ]
        )
        assert_that_result_is_successful_and_output_json_equals(
            result,
            {"status": f"Function {entity_name.upper()} successfully created."},
        )

    def snowpark_drop_should_finish_successfully(
        self,
        entity_name: str,
        arguments: str,
    ) -> None:
        result = self._setup.runner.invoke_integration(
            [
                "snowpark",
                self.test_type.value,
                "drop",
                "--name",
                entity_name,
                "--input-parameters",
                arguments,
            ]
        )
        assert_that_result_is_successful(result)

    def procedure_coverage_report_should_raise_error_when_there_is_no_coverage_report(
        self, procedure_name: str, arguments: str
    ):

        result = self._setup.runner.invoke_integration_without_format(
            [
                "snowpark",
                "procedure",
                "coverage",
                "report",
                "-n",
                procedure_name,
                "-i",
                arguments,
            ]
        )

        assert result.exit_code == 1
        assert result.json == None
        assert result.output
        assert "Aborted.\n" in result.output
        assert not os.path.exists(".coverage")

    def procedure_coverage_should_return_report_when_files_are_present_on_stage(
        self, procedure_name, arguments
    ):
        result = self._setup.runner.invoke_integration(
            [
                "snowpark",
                "procedure",
                "coverage",
                "report",
                "-n",
                procedure_name,
                "-i",
                arguments,
                "--output-format",
                "json",
            ]
        )

        assert result.exit_code == 0
        assert os.path.isfile("coverage.json")

        with open("coverage.json", "r") as coverage_file:
            coverage = json.load(coverage_file)

        assert "percent_covered" in coverage["totals"].keys()
        assert "excluded_lines" in coverage["totals"].keys()

    def coverage_clear_should_execute_succesfully(self, procedure_name, arguments):
        result = self._setup.runner.invoke_integration(
            [
                "snowpark",
                "procedure",
                "coverage",
                "clear",
                "-n",
                procedure_name,
                "-i",
                arguments,
            ]
        )

        assert result.exit_code == 0
        print(result.json)
        assert result.json["result"] == "removed"

    def assert_that_no_entities_are_in_snowflake(self) -> None:
        self.assert_that_only_these_entities_are_in_snowflake()

    def assert_that_only_these_entities_are_in_snowflake(
        self, *expected_full_entity_signatures: str
    ) -> None:
        actual_entity_signatures = [
            entity["arguments"]
            for entity in self._setup.query_entities_created_in_this_test_case()
        ]
        adjusted_expected_full_function_signatures = [
            entity.upper() for entity in expected_full_entity_signatures
        ]
        assert set(actual_entity_signatures) == set(
            adjusted_expected_full_function_signatures
        )

    def assert_that_no_files_are_staged_in_test_db(self) -> None:
        self.assert_that_only_these_files_are_staged_in_test_db()

    def assert_that_only_these_files_are_staged_in_test_db(
        self, *expected_file_paths: str
    ) -> None:
        assert set(self.get_actual_files_staged_in_db()) == set(expected_file_paths)

    def assert_that_only_app_and_coverage_file_are_staged_in_test_db(
        self, path_beggining: str
    ):
        coverage_regex = re.compile(
            path_beggining + "/coverage/[0-9]{8}-[0-9]{6}.coverage"
        )
        app_name = path_beggining + "/app.zip"

        assert app_name in (actual_file_paths := self.get_actual_files_staged_in_db())
        assert any(coverage_regex.match(file) for file in actual_file_paths)

    def get_actual_files_staged_in_db(self):
        return [
            staged_file["name"]
            for staged_file in self._setup.query_files_uploaded_in_this_test_case()
        ]

    @staticmethod
    def add_requirements_to_requirements_txt(
        requirements: List[str], file_path: str = "requirements.txt"
    ):
        if os.path.exists(file_path):
            with open(file_path, "a") as reqs_file:
                for req in requirements:
                    reqs_file.write(req + "\n")
