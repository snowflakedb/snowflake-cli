from __future__ import annotations

import re
from pathlib import Path
from textwrap import dedent
from typing import Dict, Optional, Set, cast

import jinja2
from click import ClickException
from jinja2 import Environment, StrictUndefined, UndefinedError, loaders
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectDefinition,
)
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.cli.api.utils.models import DictWithEnvironFallback

_CONTEXT_KEY = "ctx"


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


def _add_project_context(
    external_data: Dict, project_definition: ProjectDefinition
) -> Dict:
    """
    Updates the external data with variables from snowflake.yml definition file.
    """
    if _CONTEXT_KEY in external_data:
        raise ClickException(
            f"{_CONTEXT_KEY} in user defined data. The `{_CONTEXT_KEY}` variable is reserved for CLI usage."
        )

    context_data = {_CONTEXT_KEY: {"env": DictWithEnvironFallback({})}}
    # If there's project definition file then resolve variables from it
    if project_definition and project_definition.meets_version_requirement("1.1"):
        context_data = _resolve_variables_in_project(project_definition)

    return {**external_data, **context_data}


def _remove_ctx_env_prefix(text: str) -> str:
    prefix = "ctx.env."
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


def string_includes_template(text: str) -> bool:
    return bool(re.search("&{.+}", text))


def _resolve_variables_in_project(project_definition: ProjectDefinition):
    variables_data: DictWithEnvironFallback = cast(
        DictWithEnvironFallback, project_definition.env
    )

    variables_with_dependencies = _check_variables_consistency(variables_data)
    # Sort keys so we start from the shortest having lower probability of including more than one variable
    unresolved_keys = sorted(list(variables_with_dependencies), reverse=True)

    env = get_snowflake_cli_jinja_env()
    context_data = {_CONTEXT_KEY: project_definition}
    while unresolved_keys:
        key = unresolved_keys.pop()
        value = variables_data[key]
        if not isinstance(value, str):
            continue

        try:  # try to evaluate the template given current state of know variables
            new_value = env.from_string(value).render(context_data)
            if string_includes_template(new_value):
                unresolved_keys.append(key)
            variables_data[key] = env.from_string(value).render(context_data)
        except UndefinedError:
            unresolved_keys.append(key)

    return context_data


def _check_variables_consistency(variables_data: DictWithEnvironFallback):
    """
    Checks consistency of provided dictionary by
    1. checking reference cycles
    2. checking for missing variables
    """
    # Variables that are not specified in env section
    missing_variables: Set[str] = set()
    # Variables that require other variables
    variables_with_dependencies: Set[str] = set()

    for key, value in variables_data.items():
        # Templates are reserved only to string variables
        if not isinstance(value, str):
            continue

        # This value requires
        include_template, required_variables = _search_for_required_variables(value)

        if key in required_variables:
            raise ClickException(
                f"Cycle detected in environment definition for `{key}` variable."
            )

        if required_variables or include_template:
            variables_with_dependencies.add(key)

        for variable in required_variables:
            if variable not in variables_data:
                missing_variables.add(variable)

    # If there are unknown env variables then we raise an error
    if missing_variables:
        raise ClickException(
            "The following variables are used in environment definition but are not defined: {}".format(
                ", ".join(missing_variables)
            )
        )
    return variables_with_dependencies


def _search_for_required_variables(variable_value: str):
    """
    Look for &{...} pattern in  variable value. Returns a list of env variables required
    to expand this template.`
    """
    ctx_env_prefix = f"{_CONTEXT_KEY}.env."
    found_variables = re.findall(r"(&{([\.\w ]+)})+", variable_value)
    required_variables = []
    for _, variable in found_variables:
        var: str = variable.strip()
        if var.startswith(ctx_env_prefix):
            required_variables.append(var[len(ctx_env_prefix) :])
    return len(found_variables), required_variables


def snowflake_cli_jinja_render(content: str, data: Dict | None = None) -> str:
    data = _add_project_context(
        external_data=data or dict(), project_definition=cli_context.project_definition
    )
    return get_snowflake_cli_jinja_env().from_string(content).render(**data)
