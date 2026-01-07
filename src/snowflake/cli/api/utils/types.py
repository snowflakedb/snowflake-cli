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

from typing import Any, Dict

Context = Dict[str, Any]
Definition = Dict[str, Any]


def try_cast_to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    # Now if value is not string then cast it to str. Simplifies logic for 1 and 0
    if not isinstance(value, str):
        value = str(value)

    know_booleans_mapping = {"true": True, "false": False, "1": True, "0": False}

    if value.lower() not in know_booleans_mapping:
        raise ValueError(f"Could not cast {value} to bool value")
    return know_booleans_mapping[value.lower()]


def try_cast_to_int(value: Any) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError(f"Could not cast empty string to int value")
        try:
            return int(stripped)
        except ValueError:
            raise ValueError(f"Could not cast '{value}' to int value")

    try:
        return int(value)
    except (ValueError, TypeError):
        raise ValueError(f"Could not cast '{value}' to int value")
