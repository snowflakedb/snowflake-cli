from typing import Dict, List

from tests_integration.test_utils import contains_row_with
from tests_integration.conftest import CommandResult


def assert_that_result_is_successful(result: CommandResult) -> None:
    assert result.exit_code == 0


def assert_that_result_is_successful_and_output_json_contains(
    result: CommandResult,
    expected_output: Dict,
) -> None:
    assert_that_result_is_successful(result)
    assert_that_result_contains_row_with(result, expected_output)


def assert_that_result_is_successful_and_output_json_equals(
    result: CommandResult,
    expected_output: List[Dict],
) -> None:
    assert_that_result_is_successful(result)
    assert result.json == expected_output


def assert_that_result_contains_row_with(result: CommandResult, expect: Dict) -> None:
    assert result.json is not None
    assert contains_row_with(result.json, expect)


def assert_that_result_is_successful_and_done_is_on_output(
    result: CommandResult,
) -> None:
    assert_that_result_is_successful(result)
    assert result.output is not None and result.output.strip() == "Done"
