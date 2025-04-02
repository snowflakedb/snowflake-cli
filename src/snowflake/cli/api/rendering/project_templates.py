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

from typing import Any, Dict, List, Optional

from click import ClickException
from jinja2 import (
    Environment,
    StrictUndefined,
    TemplateSyntaxError,
    UndefinedError,
    loaders,
)
from snowflake.cli.api.exceptions import InvalidTemplateError
from snowflake.cli.api.rendering.jinja import IgnoreAttrEnvironment, env_bootstrap
from snowflake.cli.api.secure_path import SecurePath

_VARIABLE_TEMPLATE_START = "<!"
_VARIABLE_TEMPLATE_END = "!>"
_BLOCK_TEMPLATE_START = "<!!"
_BLOCK_TEMPLATE_END = "!!>"


def to_snowflake_identifier(value: Optional[str]) -> Optional[str]:
    if not value:
        # passing "None" through filter to allow jinja to handle "undefined value" exception
        return value

    import re

    # TODO: remove code duplication when joining "init" with "snow app init"
    # See https://docs.snowflake.com/en/sql-reference/identifiers-syntax for identifier syntax
    unquoted_identifier_regex = r"([a-zA-Z_])([a-zA-Z0-9_$]{0,254})"
    quoted_identifier_regex = r'"((""|[^"]){0,255})"'

    if re.fullmatch(quoted_identifier_regex, value):
        return value

    result = re.sub(r"[. -]+", "_", value)
    if not re.fullmatch(unquoted_identifier_regex, result):
        raise ClickException(
            f"Value '{value}' cannot be converted to valid Snowflake identifier."
            ' Consider enclosing it in double quotes: ""'
        )
    return result


PROJECT_TEMPLATE_FILTERS = [to_snowflake_identifier]


def get_template_cli_jinja_env(template_root: SecurePath) -> Environment:
    env = env_bootstrap(
        IgnoreAttrEnvironment(
            loader=loaders.FileSystemLoader(searchpath=template_root.path),
            keep_trailing_newline=True,
            variable_start_string=_VARIABLE_TEMPLATE_START,
            variable_end_string=_VARIABLE_TEMPLATE_END,
            block_start_string=_BLOCK_TEMPLATE_START,
            block_end_string=_BLOCK_TEMPLATE_END,
            undefined=StrictUndefined,
        )
    )
    env.filters["to_snowflake_identifier"] = to_snowflake_identifier

    return env


def render_template_files(
    template_root: SecurePath, files_to_render: List[str], data: Dict[str, Any]
) -> None:
    """Override all listed files with their rendered version."""
    jinja_env = get_template_cli_jinja_env(template_root)
    for path in files_to_render:
        try:
            jinja_template = jinja_env.get_template(path)
            rendered_result = jinja_template.render(**data)
            full_path = template_root / path
            full_path.write_text(rendered_result)
        except TemplateSyntaxError as err:
            raise InvalidTemplateError(
                f"Invalid template syntax in line {err.lineno} of file {path}:\n"
                f"{err.message}"
            )
        except UndefinedError as err:
            raise InvalidTemplateError(err.message)
