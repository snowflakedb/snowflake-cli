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
from textwrap import dedent
from typing import Any, Dict, Optional

import jinja2
from click import ClickException
from jinja2 import Environment, StrictUndefined, loaders
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath

CONTEXT_KEY = "ctx"
FUNCTION_KEY = "fn"


def _resolve_within_project_root(file_name: str, filter_name: str) -> Path:
    """Resolve ``file_name`` and ensure it is contained within the project root.

    Relative paths are anchored to the project root (not CWD) before resolving,
    matching the behaviour of the sibling helper in entities/utils.py.
    """
    # Lazy import to avoid circular dependency with ``cli_global_context``.
    from snowflake.cli.api.cli_global_context import get_cli_context

    root = Path(get_cli_context().project_root).resolve()
    candidate = Path(file_name).expanduser()
    if candidate.is_absolute():
        target = candidate.resolve()
    else:
        target = (root / candidate).resolve()
    try:
        target.relative_to(root)
    except ValueError:
        raise ClickException(
            f"{filter_name}: path '{file_name}' is outside the project root."
        )
    return target


def read_file_content(file_name: str) -> str:
    target = _resolve_within_project_root(file_name, "read_file_content")
    return SecurePath(target).read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB)


@jinja2.pass_environment  # type: ignore
def procedure_from_js_file(env: jinja2.Environment, file_name: str):
    target = _resolve_within_project_root(file_name, "procedure_from_js_file")
    template = env.from_string(
        dedent(
            """\
            var module = {};
            var exports = {};
            module.exports = exports;
            (function() {
            {{ code }}
            })()
            return module.exports.apply(this, arguments);
            """
        )
    )
    return template.render(
        code=SecurePath(target).read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB)
    )


_CUSTOM_FILTERS = [read_file_content, procedure_from_js_file]


def env_bootstrap(env: Environment) -> Environment:
    for custom_filter in _CUSTOM_FILTERS:
        env.filters[custom_filter.__name__] = custom_filter

    return env


class IgnoreAttrEnvironment(Environment):
    """
    extend Environment class and ignore attributes during rendering.
    This ensures that attributes of classes
    do not get used during rendering (e.g. __class__, get, etc).
    Only dict items can be used for rendering.
    """

    def getattr(self, obj, attribute):  # noqa: A003
        try:
            return obj[attribute]
        except (TypeError, LookupError, AttributeError):
            return self.undefined(obj=obj, name=attribute)

    def getitem(self, obj, argument):
        try:
            return obj[argument]
        except (AttributeError, TypeError, LookupError):
            return self.undefined(obj=obj, name=argument)


def get_basic_jinja_env(loader: Optional[loaders.BaseLoader] = None) -> Environment:
    return env_bootstrap(
        IgnoreAttrEnvironment(
            loader=loader or loaders.BaseLoader(),
            keep_trailing_newline=True,
            undefined=StrictUndefined,
        )
    )


def jinja_render_from_file(
    template_path: Path, data: Dict[str, Any], output_file_path: Optional[Path] = None
) -> Optional[str]:
    """
    Renders a jinja template and outputs either the rendered contents as string or writes to a file.

    Args:
        template_path (Path): Path to the template
        data (dict): A dictionary of jinja variables and their actual values
        output_file_path (Optional[Path]): If provided then rendered template will be written to this file

    Returns:
        None if file path is provided, else returns the rendered string.
    """
    env = get_basic_jinja_env(
        loader=loaders.FileSystemLoader(template_path.parent.as_posix())
    )
    loaded_template = env.get_template(template_path.name)
    rendered_result = loaded_template.render(**data)
    if output_file_path:
        SecurePath(output_file_path).write_text(rendered_result)
        return None
    else:
        return rendered_result
