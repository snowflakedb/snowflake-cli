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

from typing import Any, Callable


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
