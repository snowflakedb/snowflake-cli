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

import json
import os
import re
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
from zipfile import ZipFile

from syrupy import SnapshotAssertion

from tests_integration.conftest import SnowCLIRunner
from tests_integration.testing_utils import assert_that_result_is_error
from tests_integration.testing_utils.assertions.test_file_assertions import (
    assert_that_current_working_directory_contains_only_following_files,
)
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_contains_row_with,
    assert_that_result_is_successful,
    assert_that_result_is_successful_and_output_json_equals,
)
from tests_integration.testing_utils.sql_utils import SqlTestHelper

# TODO name 'entity' for both function and procedure does not seem to be perfect.
#  But i can`t come up with anything better


class TestType(Enum):
    FUNCTION = "function"
    PROCEDURE = "procedure"

    __test__ = False


class SnowparkTestSetup:
    def __init__(
        self,
        runner: SnowCLIRunner,
        sql_test_helper: SqlTestHelper,
        test_database,
        snapshot: SnapshotAssertion,
    ):
        self.runner = runner
        self.sql_test_helper = sql_test_helper
        self.snapshot = snapshot

    def query_files_uploaded_in_this_test_case(
        self, stage_name: str
    ) -> List[Dict[str, Any]]:
        return self.sql_test_helper.execute_single_sql(f"LIST @{stage_name}")


class SnowparkTestSteps:
    def __init__(self, setup: SnowparkTestSetup):
        self._setup = setup

    def object_show_should_return_no_data(
        self, object_type: str, object_prefix: str
    ) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "list",
                object_type,
                "--like",
                f"{object_prefix}%",
            ]
        )
        assert_that_result_is_successful_and_output_json_equals(result, [])

    def object_show_includes_given_identifiers(
        self,
        object_type: str,
        identifier: Tuple[str, str],
    ) -> None:
        entity_name, signature = identifier
        result = self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "list",
                object_type,
                "--like",
                f"{entity_name}%",
            ]
        )
        assert_that_result_is_successful(result)

        assert_that_result_contains_row_with(
            result,
            {
                "name": entity_name.upper(),
                "arguments": f"{entity_name}{signature}".upper(),
            },
        )

        if object_type == TestType.FUNCTION:
            assert_that_result_contains_row_with(result, {"language": "PYTHON"})

    def snowpark_execute_should_return_expected_value(
        self, object_type: str, identifier: str, expected_value: Any
    ) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "snowpark",
                "execute",
                object_type,
                identifier,
            ]
        )

        entity_name = identifier.rsplit("(")[0]

        assert_that_result_is_successful(result)
        assert_that_result_contains_row_with(
            result,
            {
                (
                    identifier.upper()
                    if object_type == TestType.FUNCTION.value
                    else entity_name.upper()
                ): expected_value
            },
        )

    def object_describe_should_return_entity_description(
        self,
        object_type: str,
        identifier: str,
        signature: str,
        returns: str,
    ) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "describe",
                object_type,
                identifier,
            ]
        )
        assert_that_result_is_successful(result)

        if returns:
            assert_that_result_contains_row_with(
                result, {"property": "returns", "value": returns}
            )
        assert_that_result_contains_row_with(
            result, {"property": "signature", "value": signature}
        )
        assert result.json is not None

    def snowpark_build_should_zip_files(self, *args, additional_files=None) -> None:
        if not additional_files:
            additional_files = []

        current_files = set(Path(".").glob("**/*"))
        result = self._setup.runner.invoke_json(
            ["snowpark", "build", "--format", "JSON", *args]
        )

        assert result.exit_code == 0, result.output
        assert result.json, result.output
        assert "message" in result.json
        assert "Build done. Artifact path:" in result.json["message"]  # type: ignore

        assert_that_current_working_directory_contains_only_following_files(
            *current_files,
            Path("app.zip"),
            *additional_files,
            Path("requirements.snowflake.txt"),
            excluded_paths=[".packages"],
        )

    def snowpark_deploy_with_coverage_wrapper_should_finish_successfully_and_return(
        self, expected_result: List[Dict[str, str]]
    ):
        return self._run_deploy(expected_result, ["--install-coverage-wrapper"])

    def snowpark_deploy_should_finish_successfully_and_return(
        self,
        expected_result: List[Dict[str, str]],
        additional_arguments: List[str] = [],
    ):
        self._run_deploy(expected_result, additional_arguments)

    def _run_deploy(
        self,
        expected_result: List[Dict[str, str]],
        additional_arguments: Optional[List[str]] = None,
    ):
        arguments = [
            "snowpark",
            "deploy",
        ]

        if additional_arguments:
            arguments.extend(additional_arguments)

        result = self._setup.runner.invoke_with_connection_json(arguments)
        assert_that_result_is_successful(result)
        assert result.json == expected_result

    def snowpark_deploy_should_return_error_with_message_contains(
        self, message_contains: str
    ):
        result = self._setup.runner.invoke_with_connection_json(
            [
                "snowpark",
                "deploy",
            ]
        )
        assert_that_result_is_error(result, 1)
        assert result.output and result.output.__contains__(message_contains)

    def object_drop_should_finish_successfully(
        self, object_type: str, identifier: str
    ) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "object",
                "drop",
                object_type,
                identifier,
            ]
        )
        assert_that_result_is_successful(result)

    def package_should_build_proper_artifact(self, package_name: str, file_name: str):
        result = self._setup.runner.invoke_with_connection_json(
            ["snowpark", "package", "create", package_name, "--pypi-download"]
        )

        assert result.exit_code == 0
        assert os.path.isfile(f"{package_name}.zip")
        assert file_name in ZipFile(f"{package_name}.zip").namelist()

    def package_should_upload_artifact_to_stage(self, file_name, stage_name):
        result = self._setup.runner.invoke_with_connection_json(
            ["snowpark", "package", "upload", "-f", file_name, "-s", stage_name]
        )

        assert result.exit_code == 0
        assert (
            f"Package {file_name} UPLOADED to Snowflake @{stage_name}/{file_name}."
            in result.json["message"]
        )

    def artifacts_left_after_package_creation_should_be_deleted(self, file_name):
        if os.path.isfile(file_name):
            os.remove(file_name)
        if os.path.isdir(".packages"):
            os.rmdir(".packages")

        assert not os.path.exists(file_name)
        assert not os.path.exists(".packages")

    def procedure_coverage_should_return_report_when_files_are_present_on_stage(
        self, identifier: str
    ):
        result = self._setup.runner.invoke_with_connection_json(
            [
                "snowpark",
                "coverage",
                "report",
                identifier,
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

    def coverage_clear_should_execute_successfully(self):
        result = self._setup.runner.invoke_with_connection_json(
            [
                "snowpark",
                "coverage",
                "clear",
            ]
        )

        assert result.exit_code == 0
        assert result.json["result"] == "removed"  # type: ignore

    def assert_those_procedures_are_in_snowflake(
        self, procedure_name: str = "", *expected_full_entity_signatures: str
    ):
        self._assert_those_objects_are_in_snowflake(
            "PROCEDURES", procedure_name, *expected_full_entity_signatures
        )

    def assert_those_functions_are_in_snowflake(
        self, function_name: str, *expected_full_entity_signatures: str
    ):
        self._assert_those_objects_are_in_snowflake(
            "FUNCTIONS", function_name, *expected_full_entity_signatures
        )

    def _assert_those_objects_are_in_snowflake(
        self, object_type: str, object_name: str, *expected_full_entity_signatures: str
    ):
        query_string = f"SHOW USER {object_type} LIKE '{object_name}'"
        result = self._setup.sql_test_helper.execute_single_sql(query_string)
        self._assert_that_only_these_entities_are_in_snowflake(
            result, *expected_full_entity_signatures
        )

    def _assert_that_only_these_entities_are_in_snowflake(
        self, query_result, *expected_full_entity_signatures: str
    ) -> None:
        actual_entity_signatures = [entity["arguments"] for entity in query_result]
        adjusted_expected_full_function_signatures = [
            entity.upper() for entity in expected_full_entity_signatures
        ]
        assert set(actual_entity_signatures) == set(
            adjusted_expected_full_function_signatures
        )

    def assert_that_only_these_files_are_staged_in_test_db(
        self,
        *expected_file_paths: str,
        stage_name: str,
    ) -> None:
        assert set(self.get_actual_files_staged_in_db(stage_name)) == set(
            expected_file_paths
        )

    def assert_that_only_app_and_coverage_file_are_staged_in_test_db(
        self, stage_path: str, artifact_name: str, stage_name: str
    ):
        coverage_regex = re.compile(stage_path + "/coverage/[0-9]{8}-[0-9]{6}.coverage")

        assert f"{stage_path}/{artifact_name}" in (
            actual_file_paths := self.get_actual_files_staged_in_db(
                stage_name=stage_name
            )
        )
        assert any(coverage_regex.match(file) for file in actual_file_paths)

    def get_actual_files_staged_in_db(self, stage_name: str):
        return [
            staged_file["name"]
            for staged_file in self._setup.query_files_uploaded_in_this_test_case(
                stage_name=stage_name
            )
        ]

    def set_grants_on_selected_object(
        self, object_type: str, object_name: str, privillege: str, role: str
    ):
        self._setup.sql_test_helper.execute_single_sql(
            f"GRANT {privillege} ON {object_type} {object_name} TO ROLE {role};"
        )

    def assert_that_object_has_expected_grant(
        self,
        object_type: str,
        object_name: str,
        expected_privillege: str,
        expected_role: str,
    ):
        result = self._setup.sql_test_helper.execute_single_sql(
            f"SHOW GRANTS ON {object_type} {object_name};"
        )
        assert any(
            [
                (
                    grant.get("privilege") == expected_privillege.upper()
                    and grant.get("grantee_name") == expected_role.upper()
                )
                for grant in result
            ]
        )
