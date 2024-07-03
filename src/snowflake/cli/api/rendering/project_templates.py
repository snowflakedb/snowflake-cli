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

from typing import Any, Dict, List

from jinja2 import Environment, StrictUndefined, loaders
from snowflake.cli.api.rendering.jinja import env_bootstrap
from snowflake.cli.api.secure_path import SecurePath

_PROJECT_TEMPLATE_START = "<!"
_PROJECT_TEMPLATE_END = "!>"


def get_template_cli_jinja_env(template_root: SecurePath) -> Environment:
    _random_block = "___very___unique___block___to___disable___logic___blocks___"
    return env_bootstrap(
        Environment(
            loader=loaders.FileSystemLoader(searchpath=template_root.path),
            keep_trailing_newline=True,
            variable_start_string=_PROJECT_TEMPLATE_START,
            variable_end_string=_PROJECT_TEMPLATE_END,
            block_start_string=_random_block,
            block_end_string=_random_block,
            undefined=StrictUndefined,
        )
    )


def render_template_files(
    template_root: SecurePath, files_to_render: List[str], data: Dict[str, Any]
) -> None:
    """Override all listed files with their rendered version."""
    jinja_env = get_template_cli_jinja_env(template_root)
    for path in files_to_render:
        jinja_template = jinja_env.get_template(path)
        rendered_result = jinja_template.render(**data)
        full_path = template_root / path
        full_path.write_text(rendered_result)
