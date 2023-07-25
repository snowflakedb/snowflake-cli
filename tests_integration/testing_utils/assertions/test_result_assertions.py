import json
from typing import Any, List, Dict

from click.testing import Result


def assert_that_result_is_successful(result: Result) -> None:
    assert result.exit_code == 0


def assert_that_result_is_successful_and_output_contains(
    result: Result,
    expected_output: str,
    ignore_case: bool = False,
) -> None:
    assert_that_result_is_successful(result)
    actual = result.output.lower() if ignore_case else result.output
    expected = expected_output.lower() if ignore_case else expected_output
    assert expected in actual


def assert_that_result_is_successful_and_output_equals(
    result: Result,
    expected_output: str,
    ignore_case: bool = False,
) -> None:
    assert_that_result_is_successful(result)
    actual = result.output.lower() if ignore_case else result.output
    expected = expected_output.lower() if ignore_case else expected_output
    assert expected == actual


def assert_that_result_is_successful_and_has_no_output(result: Result) -> None:
    assert_that_result_is_successful_and_output_equals(result, "")


def _extract_from_json_path(
    node: Any,
    relative_path_to_target: List[Any],
    absolute_path_to_current_node: List[Any],
) -> Any:
    if len(relative_path_to_target) == 0:
        return node
    path_head = relative_path_to_target[0]

    is_valid_path_for_dict = isinstance(path_head, str) and isinstance(node, Dict)
    is_valid_path_for_list = isinstance(path_head, int) and isinstance(node, List)

    if is_valid_path_for_dict or is_valid_path_for_list:
        return _extract_from_json_path(
            node=node[path_head],
            relative_path_to_target=relative_path_to_target[1:],
            absolute_path_to_current_node=absolute_path_to_current_node + [path_head],
        )
    else:
        raise RuntimeError(
            f"Type [{type(node)}] of node at path [{'.'.join(absolute_path_to_current_node)}] "
            f"does not match next part of the json path: [{path_head}]."
        )


def assert_that_json_output_contains_value_at_path(
    result: Result, path: List[Any], expected_value: Any
) -> None:
    parsed_json = json.loads(result.output)
    assert (
        _extract_from_json_path(
            node=parsed_json,
            relative_path_to_target=path,
            absolute_path_to_current_node=[],
        )
        == expected_value
    )


def assert_that_result_is_successful_and_json_output_contains_value_at_path(
    result: Result, path: List[Any], expected_value: Any
) -> None:
    assert_that_result_is_successful(result)
    assert_that_json_output_contains_value_at_path(
        result=result, path=path, expected_value=expected_value
    )
