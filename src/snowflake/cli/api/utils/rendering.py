from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from typing import Dict, Optional

import jinja2
from click import ClickException
from jinja2 import Environment, StrictUndefined, loaders
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath

CONTEXT_KEY = "ctx"
_YML_TEMPLATE_START = "<%"
_YML_TEMPLATE_END = "%>"


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


def get_snowflake_cli_jinja_env() -> Environment:
    _random_block = "___very___unique___block___to___disable___logic___blocks___"
    return _env_bootstrap(
        Environment(
            loader=loaders.BaseLoader(),
            keep_trailing_newline=True,
            variable_start_string=_YML_TEMPLATE_START,
            variable_end_string=_YML_TEMPLATE_END,
            block_start_string=_random_block,
            block_end_string=_random_block,
            undefined=StrictUndefined,
        )
    )


def get_sql_cli_jinja_env():
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
        return None
    else:
        return rendered_result


def snowflake_sql_jinja_render(content: str, data: Dict | None = None) -> str:
    data = data or {}
    if CONTEXT_KEY in data:
        raise ClickException(
            f"{CONTEXT_KEY} in user defined data. The `{CONTEXT_KEY}` variable is reserved for CLI usage."
        )

    context_data = cli_context.template_context
    context_data.update(data)
    return get_sql_cli_jinja_env().from_string(content).render(**context_data)
