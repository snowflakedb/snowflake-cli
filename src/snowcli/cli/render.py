#!/usr/bin/env python
from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent
from typing import List, Optional

import jinja2
import typer

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})


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


def _parse_key_value(key_value_str: str):
    parts = key_value_str.split("=")
    if len(parts) < 2:
        raise ValueError("Passed key-value pair does not comform with key=value format")

    return parts[0], "=".join(parts[1:])


@app.command("template")
def render_template(
    template_path: Path = typer.Argument(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to template file",
    ),
    data_file_path: Optional[Path] = typer.Option(
        None,
        "--data-file",
        "-d",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to JSON file with data that will be passed to the template",
    ),
    data_override: Optional[List[str]] = typer.Option(
        None,
        "-D",
        help="String in format of key=value that will be passed to rendered template. "
        "If used together with data file then this will override existing values",
    ),
    output_file_path: Optional[Path] = typer.Option(
        None,
        "--output-file",
        "-o",
        dir_okay=False,
        help="If provided then rendered template will be written to this file",
    ),
):
    """Renders Jinja2 template. Can be used to construct complex SQL files."""
    data = {}
    if data_file_path:
        data = json.loads(data_file_path.read_text())
    if data_override:
        for key_value_str in data_override:
            key, value = _parse_key_value(key_value_str)
            data[key] = value

    env = jinja2.Environment(
        loader=jinja2.loaders.FileSystemLoader(template_path.parent)
    )
    filters = [render_metadata, read_file_content, procedure_from_js_file]
    for custom_filter in filters:
        env.filters[custom_filter.__name__] = custom_filter

    template = env.from_string(template_path.read_text())
    result = template.render(**data)
    if output_file_path:
        output_file_path.write_text(result)
    else:
        print(result)
