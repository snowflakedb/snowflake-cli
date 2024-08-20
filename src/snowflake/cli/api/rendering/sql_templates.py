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

from pathlib import Path
from typing import Any, Dict, Optional

from click import ClickException
from jinja2 import StrictUndefined, loaders
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.rendering.jinja import (
    CONTEXT_KEY,
    FUNCTION_KEY,
    IgnoreAttrEnvironment,
    env_bootstrap,
)

_PRIMARY_TEMPLATE_START = "<%"
_PRIMARY_TEMPLATE_END = "%>"
_SECONDARY_TEMPLATE_START = "&{"
_SECONDARY_TEMPLATE_END = "}"
RESERVED_KEYS = [CONTEXT_KEY, FUNCTION_KEY]


class SqlTemplateEnv:
    """Class joining two jinja environments."""

    def __init__(self, *, loader: Optional[loaders.BaseLoader] = None):
        _random_block = "___very___unique___block___to___disable___logic___blocks___"
        common_kwargs = dict(
            keep_trailing_newline=True,
            block_start_string=_random_block,
            block_end_string=_random_block,
            undefined=StrictUndefined,
        )

        self.primary_env = env_bootstrap(
            IgnoreAttrEnvironment(
                loader=loader or loaders.BaseLoader(),
                variable_start_string=_PRIMARY_TEMPLATE_START,
                variable_end_string=_PRIMARY_TEMPLATE_END,
                **common_kwargs,
            )
        )
        self.secondary_env = env_bootstrap(
            IgnoreAttrEnvironment(
                loader=loaders.BaseLoader(),
                variable_start_string=_SECONDARY_TEMPLATE_START,
                variable_end_string=_SECONDARY_TEMPLATE_END,
                **common_kwargs,
            )
        )

    def _render_from_secondary_env(self, template: str, data: Dict[str, Any]) -> str:
        return self.secondary_env.from_string(template).render(**data)

    def render_from_string(self, template: str, data: Dict[str, Any]) -> str:
        first_rendering = self.primary_env.from_string(template).render(**data)
        return self._render_from_secondary_env(first_rendering, data)

    def render_from_file(self, template_file_path: Path, data: Dict[str, Any]) -> str:
        first_rendering = self.primary_env.get_template(str(template_file_path)).render(
            **data
        )
        return self._render_from_secondary_env(first_rendering, data)


def snowflake_sql_jinja_render(content: str, data: Dict | None = None) -> str:
    data = data or {}

    for reserved_key in RESERVED_KEYS:
        if reserved_key in data:
            raise ClickException(
                f"{reserved_key} in user defined data. The `{reserved_key}` variable is reserved for CLI usage."
            )

    context_data = get_cli_context().template_context
    context_data.update(data)
    return SqlTemplateEnv().render_from_string(content, context_data)
