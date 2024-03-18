from __future__ import annotations

import os
from pathlib import Path
from textwrap import dedent
from typing import Dict, Optional

import jinja2
from jinja2 import Environment, StrictUndefined, loaders
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath


def read_file_content(file_name: str):
    return SecurePath(file_name).read_text(file_size_limit_mb=UNLIMITED)


@jinja2.pass_environment  # type: ignore
def procedure_from_js_file(env: jinja2.Environment, file_name: str):
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
        code=SecurePath(file_name).read_text(file_size_limit_mb=UNLIMITED)
    )


_CUSTOM_FILTERS = [read_file_content, procedure_from_js_file]


def _env_bootstrap(env: Environment) -> Environment:
    for custom_filter in _CUSTOM_FILTERS:
        env.filters[custom_filter.__name__] = custom_filter

    return env


def get_snowflake_cli_jinja_env():
    _random_block = "___very___unique___block___to___disable___logic___blocks___"
    return _env_bootstrap(
        Environment(
            loader=loaders.BaseLoader(),
            keep_trailing_newline=True,
            variable_start_string="&{",
            variable_end_string="}",
            block_start_string=_random_block,
            block_end_string=_random_block,
            undefined=StrictUndefined,
        )
    )


def jinja_render_from_file(
    template_path: Path, data: Dict, output_file_path: Optional[Path] = None
):
    """
    Create a file from a jinja template.

    Args:
        template_path (Path): Path to the template
        data (dict): A dictionary of jinja variables and their actual values
        output_file_path (Optional[Path]): If provided then rendered template will be written to this file

    Returns:
        None
    """
    env = _env_bootstrap(
        Environment(
            loader=loaders.FileSystemLoader(template_path.parent),
            keep_trailing_newline=True,
            undefined=StrictUndefined,
        )
    )
    loaded_template = env.get_template(template_path.name)
    rendered_result = loaded_template.render(**data)
    if output_file_path:
        SecurePath(output_file_path).write_text(rendered_result)
    else:
        print(rendered_result)


class _AttrGetter:
    def __init__(self, data_dict):
        self._data_dict = data_dict

    def __getattr__(self, item):
        if item not in self._data_dict:
            raise AttributeError(f"No attribute {item}")
        return self._data_dict[item]


def _add_project_context(data: Dict):
    context_key = "ctx"
    if context_key in data:
        raise ValueError(f"{context_key} in user defined data")
    context_data = {context_key: {"env": _AttrGetter(os.environ)}}
    return {**data, **context_data}


def snowflake_cli_jinja_render(content: str, data: Dict | None = None) -> str:
    data = _add_project_context(data or dict())
    return get_snowflake_cli_jinja_env().from_string(content).render(**data)
