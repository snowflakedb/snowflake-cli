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

import typer
import yaml
from click import ClickException
from snowflake.cli.api.commands.flags import (
    NoInteractiveOption,
    VariablesOption,
    parse_key_value_variables,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
)
from snowflake.cli.api.project.schemas.template import Template, TemplateVariable
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils.rendering import get_template_cli_jinja_env

# simple Typer with defaults because it won't become a command group as it contains only one command
app = SnowTyperFactory()

# TODO: create repo and override this parameter
DEFAULT_SOURCE = "undefined"


NameArgument = typer.Argument(
    help="which subdirectory of SOURCE should be used to create a template",
    show_default=False,
)
SourceOption = typer.Option(
    default=DEFAULT_SOURCE,
    help=f"local path to template directory or URL to git repository with templates.",
)

TEMPLATE_METADATA_FILE_NAME = "template.yml"


def _fetch_local_template(
    template_source: SecurePath, path: str, dest: SecurePath
) -> SecurePath:
    template_origin = template_source / path
    if not template_origin.exists():
        raise ClickException(
            f"Template '{path}' cannot be found under {template_source.path}"
        )
    template_origin.copy(dest.path)
    return dest / template_origin.name


def _read_template_metadata(template_root: SecurePath) -> Template:
    template_metadata_path = template_root / TEMPLATE_METADATA_FILE_NAME
    if not template_metadata_path.exists():
        raise FileNotFoundError("Template does not have template.yml file")
    with template_metadata_path.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
        return Template(template_root, **yaml.safe_load(fd))


def _prompt_for_value(variable: TemplateVariable, no_interactive: bool) -> Any:
    if no_interactive:
        if not variable.default:
            raise ClickException(f"Cannot determine value of variable {variable.name}")
        return variable.default

    # override "unchecked type" with 'str', as Typer deduces type from the value of 'default'
    type_ = variable.type.python_type if variable.type else str
    prompt = variable.prompt if variable.prompt else variable.name
    return typer.prompt(prompt, default=variable.default, type=type_)


def _determine_variable_values(
    variables_metadata: List[TemplateVariable],
    variables_from_flags: Dict[str, Any],
    no_interactive: bool,
) -> Dict[str, Any]:
    variable_values = dict(variables_from_flags)

    for v in variables_metadata:
        if v.name in variable_values:
            if v.type:
                # convert value to required type
                variable_values[v] = v.type.python_type(variable_values[v.name])
            continue

        value = _prompt_for_value(v, no_interactive)
        variable_values[v.name] = value

    return variable_values


def _render_template(template_root: SecurePath, files: List[str], data: Dict[str, Any]):
    """Override all listed files with their rendered version."""
    jinja_env = get_template_cli_jinja_env(template_root)
    for path in files:
        jinja_template = jinja_env.get_template(path)
        rendered_result = jinja_template.render(**data)
        full_path = template_root / path
        full_path.write_text(rendered_result)


@app.command(no_args_is_help=True)
def init(
    name: str = NameArgument,
    template_source: Optional[str] = SourceOption,
    variables: Optional[List[str]] = VariablesOption,
    no_interactive: bool = NoInteractiveOption,
    **options,
) -> CommandResult:
    """
    Creates project from template.
    """
    variables_from_flags = {
        v.key: v.value
        for v in parse_key_value_variables(variables if variables else [])
    }

    is_remote_source = not SecurePath(template_source).exists()

    # copy/download template into tmpdir, so it is going to be removed in case command ens with an error
    with SecurePath.temporary_directory() as tmpdir:
        if is_remote_source:
            # assume template is URL
            raise NotImplementedError("urls not supported (yet)")

        else:
            template_root = _fetch_local_template(
                template_source=SecurePath(template_source), path=name, dest=tmpdir
            )

        template_metadata = _read_template_metadata(template_root)
        variable_values = _determine_variable_values(
            variables_metadata=template_metadata.variables,
            variables_from_flags=variables_from_flags,
            no_interactive=no_interactive,
        )
        _render_template(
            template_root=template_root,
            files=template_metadata.files,
            data=variable_values,
        )
        template_root.copy(".")
    return MessageResult("OK")
