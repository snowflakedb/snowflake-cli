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

import pytest
from snowflake.cli.api.utils.dict_utils import (
    deep_merge_dicts,
    remove_key_from_nested_dict_if_exists,
    traverse,
)


def test_merge_dicts_empty():
    test_dict = {}
    deep_merge_dicts(test_dict, {})
    assert test_dict == {}


def test_merge_dicts_recursive_map():
    # fmt: off
    test_dict = {
        "a": "a1",
        "b": "b1",
        "c": {
            "d": "d1",
            "e": "e1"
        }
    }
    deep_merge_dicts(test_dict, {
        "c": {
            "d": "d2",
            "a": "a2"
        }
    })
    assert test_dict == {
        "a": "a1",
        "b": "b1",
        "c": {
            "d": "d2",
            "e": "e1",
            "a": "a2"
        }
    }
    # fmt: on


def test_merge_dicts_recursive_scalar_replace_map():
    # fmt: off
    test_dict = {
        "a": "a1",
        "b": "b1",
        "c": {
            "d": "d1",
            "e": "e1"
        }
    }
    deep_merge_dicts(test_dict, {"c": "c2"})
    assert test_dict == {
        "a": "a1",
        "b": "b1",
        "c": "c2"
    }
    # fmt: on


def test_merge_dicts_recursive_two_arrays():
    # fmt: off
    test_dict = {
        "a": "a1",
        "b": {
            "c": ["c1", "c2", "c3"],
            "d": "d1"
        }
    }
    deep_merge_dicts(test_dict, {
        "b": {
            "c": ["c4", "c5"]
        }
    })
    assert test_dict == {
        "a": "a1",
        "b": {
            "c": ["c4", "c5"],
            "d": "d1"
        }
    }
    # fmt: on


def test_traverse_on_map():
    test_struct = {
        "scalar_key": "hello",
        "map_key": {"key1": "value1", "key2": "value2", "key3": True},
        "array_key": ["array1", "array2", 333, {"nestedKey1": "nestedVal1"}],
    }

    visited_elements = []

    def visit_action(element):
        visited_elements.append(element)

    traverse(test_struct, visit_action)

    expected = [
        "hello",
        "value1",
        "value2",
        True,
        "array1",
        "array2",
        333,
        "nestedVal1",
    ]
    # We do not need to guarantee order
    assert len(visited_elements) == len(expected)
    assert set(visited_elements) == set(expected)


def test_traverse_on_list():
    test_struct = ["val1", 123, False, {"mapKey1": "mapVal1"}]

    visited_elements = []

    def visit_action(element):
        visited_elements.append(element)

    traverse(test_struct, visit_action)

    expected = ["val1", 123, False, "mapVal1"]
    # We do not need to guarantee order
    assert len(visited_elements) == len(expected)
    assert set(visited_elements) == set(expected)


def test_traverse_on_scalar():
    test_struct = 444

    visited_elements = []

    def visit_action(element):
        visited_elements.append(element)

    traverse(test_struct, visit_action)

    assert visited_elements == [444]


def test_traverse_with_updates():
    test_struct = {
        "scalar_key": "hello",
        "map_key": {"key1": "value1", "key3": True},
        "array_key": ["array1", 333, {"nestedKey1": "nestedVal1"}],
    }

    def update_action(element):
        if isinstance(element, str):
            return element + "_"
        else:
            return None

    traverse(test_struct, update_action=update_action)

    assert test_struct == {
        "scalar_key": "hello_",
        "map_key": {"key1": "value1_", "key3": None},
        "array_key": ["array1_", None, {"nestedKey1": "nestedVal1_"}],
    }


def test_remove_key_from_nested_dict_if_exists():
    d = {
        "a": {
            "a1": "a1_value",
        },
        "b": "b_value",
        "c": {
            "c1": "c1_value",
            "c2": {
                "c2a": "c2a_value",
                "c2b": "c2b_value",
            },
        },
    }

    result = remove_key_from_nested_dict_if_exists(d, ["a"])
    assert result is True
    assert list(d.keys()) == ["b", "c"]

    result = remove_key_from_nested_dict_if_exists(d, ["b"])
    assert result is True
    assert list(d.keys()) == ["c"]

    with pytest.raises(ValueError):
        remove_key_from_nested_dict_if_exists(d, ["c", "c1", "c1_value_is_not_a_dict"])

    result = remove_key_from_nested_dict_if_exists(d, ["c", "c1"])
    assert result is True
    assert d["c"] == {
        "c2": {
            "c2a": "c2a_value",
            "c2b": "c2b_value",
        }
    }

    result = remove_key_from_nested_dict_if_exists(d, ["c", "c2", "non_existing"])
    assert result is False

    result = remove_key_from_nested_dict_if_exists(d, ["non_existing", "key"])
    assert result is False

    result = remove_key_from_nested_dict_if_exists(d, ["c", "c2", "c2a"])
    assert result is True
    assert d["c"] == {
        "c2": {
            "c2b": "c2b_value",
        }
    }

    result = remove_key_from_nested_dict_if_exists(d, ["c", "c2", "c2b"])
    assert result is True
    assert d == {}
