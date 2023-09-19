from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent
from typing import Optional

import jinja2


def read_file_content(file_name: str):
    return Path(file_name).read_text()


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
    return template.render(code=Path(file_name).read_text())


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
    metadata = json.loads(Path(file_name).absolute().read_text())
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


def generic_render_template(
    template_path: Path, data: dict, output_file_path: Optional[Path] = None
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
    env = jinja2.Environment(
        loader=jinja2.loaders.FileSystemLoader(template_path.parent),
        keep_trailing_newline=True,
        undefined=jinja2.StrictUndefined,
    )
    filters = [render_metadata, read_file_content, procedure_from_js_file]
    for custom_filter in filters:
        env.filters[custom_filter.__name__] = custom_filter
    loaded_template = env.get_template(template_path.name)
    rendered_result = loaded_template.render(**data)
    if output_file_path:
        output_file_path.write_text(rendered_result)
    else:
        print(rendered_result)
