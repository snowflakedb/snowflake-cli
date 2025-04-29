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
from jinja2 import Environment, StrictUndefined, loaders, meta
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.exceptions import InvalidTemplateError
from snowflake.cli.api.metrics import CLICounterField
from snowflake.cli.api.rendering.jinja import (
    CONTEXT_KEY,
    FUNCTION_KEY,
    IgnoreAttrEnvironment,
    env_bootstrap,
)

_SQL_TEMPLATE_START = "<%"
_SQL_TEMPLATE_END = "%>"
_OLD_SQL_TEMPLATE_START = "&{"
_OLD_SQL_TEMPLATE_END = "}"
RESERVED_KEYS = [CONTEXT_KEY, FUNCTION_KEY]


def _get_sql_jinja_env(template_start: str, template_end: str) -> Environment:
    _random_block = "___very___unique___block___to___disable___logic___blocks___"
    return env_bootstrap(
        IgnoreAttrEnvironment(
            variable_start_string=template_start,
            variable_end_string=template_end,
            loader=loaders.BaseLoader(),
            block_start_string=_random_block,
            block_end_string=_random_block,
            keep_trailing_newline=True,
            undefined=StrictUndefined,
        )
    )


def _does_template_have_env_syntax(env: Environment, template_content: str) -> bool:
    template = env.parse(template_content)
    return bool(meta.find_undeclared_variables(template))


def has_sql_templates(template_content: str) -> bool:
    return (
        _OLD_SQL_TEMPLATE_START in template_content
        or _SQL_TEMPLATE_START in template_content
    )


def choose_sql_jinja_env_based_on_template_syntax(
    template_content: str, reference_name: Optional[str] = None
) -> Environment:
    old_syntax_env = _get_sql_jinja_env(_OLD_SQL_TEMPLATE_START, _OLD_SQL_TEMPLATE_END)
    new_syntax_env = _get_sql_jinja_env(_SQL_TEMPLATE_START, _SQL_TEMPLATE_END)
    has_old_syntax = _does_template_have_env_syntax(old_syntax_env, template_content)
    has_new_syntax = _does_template_have_env_syntax(new_syntax_env, template_content)
    reference_name_str = f" in {reference_name}" if reference_name else ""
    if has_old_syntax and has_new_syntax:
        raise InvalidTemplateError(
            f"The SQL query{reference_name_str} mixes {_OLD_SQL_TEMPLATE_START} ... {_OLD_SQL_TEMPLATE_END} syntax"
            f" and {_SQL_TEMPLATE_START} ... {_SQL_TEMPLATE_END} syntax."
        )
    if has_old_syntax:
        cli_console.warning(
            f"Warning: {_OLD_SQL_TEMPLATE_START} ... {_OLD_SQL_TEMPLATE_END} syntax{reference_name_str}"
            " is deprecated and will no longer be supported."
            f" Use {_SQL_TEMPLATE_START} ... {_SQL_TEMPLATE_END} syntax instead."
        )
        return old_syntax_env
    return new_syntax_env


def snowflake_sql_jinja_render(content: str, data: Dict | None = None) -> str:
    data = data or {}

    for reserved_key in RESERVED_KEYS:
        if reserved_key in data:
            raise ClickException(
                f"{reserved_key} in user defined data. The `{reserved_key}` variable is reserved for CLI usage."
            )

    context_data = get_cli_context().template_context
    context_data.update(data)
    env = choose_sql_jinja_env_based_on_template_syntax(content)

    get_cli_context().metrics.set_counter(
        CLICounterField.SQL_TEMPLATES, int(has_sql_templates(content))
    )

    return env.from_string(content).render(context_data)
