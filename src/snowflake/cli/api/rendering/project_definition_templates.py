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

from jinja2 import Environment, StrictUndefined, loaders
from snowflake.cli.api.rendering.jinja import (
    IgnoreAttrEnvironment,
    env_bootstrap,
)

_YML_TEMPLATE_START = "<%"
_YML_TEMPLATE_END = "%>"


def get_project_definition_cli_jinja_env() -> Environment:
    _random_block = "___very___unique___block___to___disable___logic___blocks___"
    return env_bootstrap(
        IgnoreAttrEnvironment(
            loader=loaders.BaseLoader(),
            keep_trailing_newline=True,
            variable_start_string=_YML_TEMPLATE_START,
            variable_end_string=_YML_TEMPLATE_END,
            block_start_string=_random_block,
            block_end_string=_random_block,
            undefined=StrictUndefined,
        )
    )
