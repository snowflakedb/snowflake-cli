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

import json
import re
from typing import Dict, List, Optional, Union

from tests_integration.conftest import CommandResult
from tests_integration.test_utils import contains_row_with


_BOX_LINE_PATTERN = re.compile(r"\s*\u2502(?P<content>.*)\u2502\s*")


def extract_error_box_message(raw_output: Optional[str]) -> str:
    if not raw_output:
        return ""
    raw_lines = raw_output.splitlines()
    in_box = False
    error_lines: List[str] = []
    for raw_line in raw_lines:
        line = raw_line.strip()
        if "─ Error ─" in line:
            # top of the box
            in_box = True
        elif in_box and "─────" in line:
            # bottom of the box
            return " ".join(error_lines)
        elif in_box:
            match = _BOX_LINE_PATTERN.match(line)
            if match:
                error_lines.append(match.group("content").strip())
            else:
                raise RuntimeError(f"Unexpected line in box: {line}")

    return ""


def assert_that_result_is_successful(result: CommandResult) -> None:
    assert result.exit_code == 0, result.output


def assert_that_result_is_error(result: CommandResult, expected_exit_code: int) -> None:
    assert result.exit_code == expected_exit_code, result.output


def assert_successful_result_message(result: CommandResult, expected_msg: str) -> None:
    assert result.exit_code == 0, result.output
    assert result.output == expected_msg + "\n"


def assert_that_result_is_usage_error(
    result: CommandResult, expected_error_message: str
) -> None:
    assert result.output is not None
    result_output = re.sub(r"\s*││\s*", " ", result.output.replace("\n", ""))  # type: ignore
    assert result.exit_code == 2, result.output
    assert expected_error_message in result_output, result.output


def assert_that_result_is_successful_and_output_json_contains(
    result: CommandResult,
    expected_output: Dict,
) -> None:
    assert_that_result_is_successful(result)
    assert_that_result_contains_row_with(result, expected_output)


def assert_that_result_is_successful_and_output_json_equals(
    result: CommandResult,
    expected_output: Union[Dict, List],
) -> None:
    assert_that_result_is_successful(result)
    assert result.json == expected_output


def assert_that_result_contains_row_with(result: CommandResult, expect: Dict) -> None:
    assert result.json is not None
    assert contains_row_with(result.json, expect)  # type: ignore


def assert_that_result_is_successful_and_done_is_on_output(
    result: CommandResult,
) -> None:
    assert_that_result_is_successful(result)
    assert result.output is not None
    assert json.loads(result.output) == {"message": "Done"}


def assert_that_result_is_successful_and_executed_successfully(
    result: CommandResult, is_json: bool = False
) -> None:
    """
    Checks that the command result is {"status": "Statement executed successfully"} as either json or text output.
    """
    assert_that_result_is_successful(result)
    if is_json:
        success_message = {"status": "Statement executed successfully."}
        assert result.json is not None
        if isinstance(result.json, dict):
            assert result.json == success_message
        else:
            assert len(result.json) == 1
            assert result.json[0] == success_message
    else:
        assert result.output is not None
        assert "status" in result.output
        assert "Statement executed successfully" in result.output


def assert_that_result_failed_with_message_containing(
    result: CommandResult, msg: str
) -> None:
    assert result.exit_code != 0, result.output
    assert msg in extract_error_box_message(result.output)
