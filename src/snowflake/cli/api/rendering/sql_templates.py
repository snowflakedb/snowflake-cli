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

from typing import Dict, Optional

from click import ClickException
from jinja2 import StrictUndefined, loaders
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.rendering.jinja import (
    CONTEXT_KEY,
    IgnoreAttrEnvironment,
    env_bootstrap,
)

_SQL_TEMPLATE_START = "&{"
_SQL_TEMPLATE_END = "}"


def get_sql_cli_jinja_env(*, loader: Optional[loaders.BaseLoader] = None):
    _random_block = "___very___unique___block___to___disable___logic___blocks___"
    return env_bootstrap(
        IgnoreAttrEnvironment(
            loader=loader or loaders.BaseLoader(),
            keep_trailing_newline=True,
            variable_start_string=_SQL_TEMPLATE_START,
            variable_end_string=_SQL_TEMPLATE_END,
            block_start_string=_random_block,
            block_end_string=_random_block,
            undefined=StrictUndefined,
        )
    )


def snowflake_sql_jinja_render(content: str, data: Dict | None = None) -> str:
    data = data or {}
    if CONTEXT_KEY in data:
        raise ClickException(
            f"{CONTEXT_KEY} in user defined data. The `{CONTEXT_KEY}` variable is reserved for CLI usage."
        )

    context_data = cli_context.template_context
    context_data.update(data)
    return get_sql_cli_jinja_env().from_string(content).render(**context_data)
