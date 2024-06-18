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

import os
from typing import Any, Dict

from snowflake.cli.api.exceptions import InvalidTemplate


def _validate_env(current_env: dict):
    if not isinstance(current_env, dict):
        raise InvalidTemplate(
            "env section in project definition file should be a mapping"
        )
    for variable, value in current_env.items():
        if value is None or isinstance(value, (dict, list)):
            raise InvalidTemplate(
                f"Variable {variable} in env section or project definition file should be a scalar"
            )


class EnvironWithDefinedDictFallback(Dict):
    def __init__(self, dict_input: dict):
        _validate_env(dict_input)
        super().__init__(dict_input)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as e:
            raise AttributeError(e)

    def __getitem__(self, item):
        if item in os.environ:
            return os.environ[item]
        return super().__getitem__(item)

    def __contains__(self, item):
        return item in os.environ or super().__contains__(item)

    def update_from_dict(self, update_values: Dict[str, Any]):
        return super().update(update_values)
