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
        raise ValueError(f"Could not case {value} to bool value")
    return know_booleans_mapping[value.lower()]
