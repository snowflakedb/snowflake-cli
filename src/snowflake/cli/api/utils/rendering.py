from __future__ import annotations

import os
import re
from pathlib import Path
from textwrap import dedent
from typing import Dict, Optional, List, Set

import jinja2
from click import ClickException
from jinja2 import Environment, StrictUndefined, loaders, UndefinedError

from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.project.schemas.project_definition import Variable
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


class _ContextGetter:
    def __init__(self, data_dict):
        self._data_dict = data_dict

    def __getattr__(self, item: str):
        # To align with best practices we are searching only for upper-case environment variables.
        if (key := item.upper()) in os.environ:
            return os.environ[key]
        if item not in self._data_dict:
            raise AttributeError(f"No value found for {item} in environment.")
        return self._data_dict[item]

    def __repr__(self):
        return str(self._data_dict)


def _add_project_context(external_data: Dict):
    context_key = "ctx"
    if context_key in external_data:
        raise ClickException(f"{context_key} in user defined data. The `{context_key}` variable is being used by the CLI.")

    variables = cli_context.project_definition.env
    variables_data = {v.name: v.value for v in variables}

    context_data = {
        context_key: {
            "env": _ContextGetter(data_dict=variables_data)
        }
    }
    _resolve_variables_in_project(context_data, variables_data)
    return {**external_data, **context_data}



def _remove_prefix(text):
    prefix = "ctx.env."
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def _includes_variable(value: str) -> bool:
    return bool(re.search("&{.+}", value))


def _resolve_variables_in_project(context_data: Dict, variable_data: Dict):
    env = get_snowflake_cli_jinja_env()

    # Variables that are missing from definition
    missing_variables: Set[str] = set()

    # Variables that require other variables
    variables_with_dependencies: Set[str] = set()

    for key, value in variable_data.items():
        if not isinstance(value, str):
            continue
        found_variables = re.findall('&{(.+)}', value)
        required_variables = [_remove_prefix(v.strip()) for v in found_variables]

        if required_variables:
            variables_with_dependencies.add(key)

        for variable in required_variables:
            if variable not in variable_data:
                missing_variables.add(variable)

    # Expand the data using environment variables
    for key in missing_variables:
        if key.upper() in os.environ:
            variable_data[key] = os.environ[key.upper()]
            missing_variables.remove(key)

    # If after env vars expand there are unknown variables then we raise an error
    if missing_variables:
        raise ClickException(
            "The following variables are used in project definition but are not defined: {}".format(
                ", ".join(missing_variables)
            )
        )

    # Sort keys so we start from the shortest having lower probability of including more than one variable
    unresolved_keys = sorted(list(variables_with_dependencies), reverse=True)
    while unresolved_keys:
        key = unresolved_keys.pop()
        value = variable_data[key]
        if not isinstance(value, str):
            continue

        try:  # try to evaluate the template given current state of know variables
            new_value = env.from_string(value).render(context_data)
            if _includes_variable(new_value):
                unresolved_keys.append(key)
            variable_data[key] = env.from_string(value).render(context_data)
        except UndefinedError:
            unresolved_keys.append(key)


def snowflake_cli_jinja_render(content: str, data: Dict | None = None) -> str:
    data = _add_project_context(data or dict())
    return get_snowflake_cli_jinja_env().from_string(content).render(**data)
