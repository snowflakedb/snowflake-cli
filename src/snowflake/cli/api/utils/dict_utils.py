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

from typing import Any, Callable, Dict, List, Union


def deep_merge_dicts(
    original_values: dict[Any, Any], override_values: dict[Any, Any]
) -> None:
    """
    Takes 2 dictionaries as input: original and override. The original dictionary is modified.

    For every key in the override dictionary, override the same key
    in the original dictionary, or create a new one if the key is not present.

    If the override value and the original value are both dictionaries,
    instead of overriding, this function recursively calls itself to merge the keys of the sub-dictionaries.
    """
    if not isinstance(override_values, dict) or not isinstance(original_values, dict):
        raise ValueError("Arguments are not of type dict")

    for field, value in override_values.items():
        if (
            field in original_values
            and isinstance(original_values[field], dict)
            and isinstance(value, dict)
        ):
            deep_merge_dicts(original_values[field], value)
        else:
            original_values[field] = value


def traverse(
    element: Any,
    visit_action: Callable[[Any], None] = lambda element: None,
    update_action: Callable[[Any], Any] = lambda element: element,
) -> Any:
    """
    Traverse a nested structure (lists, dicts, scalars).

    On traversal, it allows for actions or updates on each visit.

    visit_action: caller can provide a function to execute on each scalar element in the structure (leaves of the tree).
    visit_action accepts an element (scalar) as input. Return value is ignored.

    update_action: caller can provide a function to update each scalar element in the structure.
    update_action accepts an element (scalar) as input, and returns the modified value.

    """
    if isinstance(element, dict):
        for key, value in element.items():
            element[key] = traverse(value, visit_action, update_action)
        return element
    elif isinstance(element, list):
        for index, value in enumerate(element):
            element[index] = traverse(value, visit_action, update_action)
        return element
    else:
        visit_action(element)
        return update_action(element)


_NestedDict = Dict[str, Union[Any, "_NestedDict"]]


def remove_key_from_nested_dict_if_exists(
    root_dict: _NestedDict, key_path: List[str]
) -> bool:
    """
    Removes a key from a nested dictionary, if it exists.
    Removes all parents that become empty.

    :return: True if the key was removed, False if it did not exist.
    :raises ValueError: If a key in the path, besides the last one, was present but did not point to a dictionary.
    """
    path = [root_dict]
    for key in key_path:
        curr_dict = path[-1]
        if key not in curr_dict:
            return False

        child_dict = curr_dict[key]
        if not isinstance(child_dict, dict) and len(path) < len(key_path):
            raise ValueError(
                f"Expected a dictionary at key '{key}', but got {str(type(child_dict))}."
            )

        path.append(child_dict)

    # Remove the target node, and any parents that become empty
    is_target = True
    for curr_key, curr_dict, child_dict in zip(
        reversed(key_path), reversed(path[:-1]), reversed(path[1:])
    ):
        if is_target or len(child_dict) == 0:
            del curr_dict[curr_key]
            is_target = False
        else:
            break

    return True
