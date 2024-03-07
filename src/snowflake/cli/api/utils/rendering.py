from __future__ import annotations

import json
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


PROCEDURE_TEMPLATE = dedent(
    """\
    CREATE OR REPLACE {{ object_type | upper }} {{ name | upper }}(\
    {% for arg in signature %}
    {{ arg['name'] | upper }} {{ arg['type'] }}{{ "," if not loop.last -}}
    {% endfor %}
    )
    RETURNS {{ returns }}
    LANGUAGE {{ language }}
    {% if runtime_version is defined -%}
    RUNTIME_VERSION = '{{ runtime_version }}'
    {% endif -%}
    {% if packages is defined -%}
    PACKAGES = ('{{ packages }}')
    {% endif -%}
    {% if imports is defined -%}
    IMPORTS = ({% for import in imports %}'{{ import }}'{{ ", " if not loop.last }}{% endfor %})
    {% endif -%}
    {% if handler is defined -%}
    HANDLER = '{{ handler }}'
    {% endif -%}
    {% if code is defined -%}
    AS
    $$
    {{ code }}
    $$
    {%- endif -%}
    ;

    {%- if grants is defined -%}
    {%- for grant in grants %}
    GRANT USAGE ON {{ object_type | upper }} {{ name | upper }}({% for arg in signature %}{{ arg['type'] }}{{ ", " if not loop.last }}{% endfor %})
    TO DATABASE ROLE {{ grant['role'] }};
    {% endfor -%}
    {% endif -%}\
"""
)


@jinja2.pass_environment  # type: ignore
def render_metadata(env: jinja2.Environment, file_name: str):
    metadata = json.loads(
        SecurePath(file_name).absolute().read_text(file_size_limit_mb=UNLIMITED)
    )
    template = env.from_string(PROCEDURE_TEMPLATE)

    rendered = []
    known_objects = {
        "procedures": "procedure",
        "udfs": "function",
        "udtfs": "function",
    }
    for object_key, object_type in known_objects.items():
        for obj in metadata.get(object_key, []):
            rendered.append(template.render(object_type=object_type, **obj))
    return "\n".join(rendered)


_CUSTOM_FILTERS = [render_metadata, read_file_content, procedure_from_js_file]


def _env_bootstrap(env: Environment) -> Environment:
    for custom_filter in _CUSTOM_FILTERS:
        env.filters[custom_filter.__name__] = custom_filter

    return env


_RANDOM_BLOCK = "___very___unique___block___to___disable___logic___blocks___"
SNOWFLAKE_CLI_JINJA_ENV = _env_bootstrap(
    Environment(
        loader=loaders.BaseLoader(),
        keep_trailing_newline=True,
        variable_start_string="%{",
        variable_end_string="}",
        block_start_string=_RANDOM_BLOCK,
        block_end_string=_RANDOM_BLOCK,
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


def snowflake_cli_jinja_render(content: str, data: Dict | None = None) -> str:
    data = data or dict()
    return SNOWFLAKE_CLI_JINJA_ENV.from_string(content).render(**data)
