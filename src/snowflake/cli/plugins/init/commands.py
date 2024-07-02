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

DEFAULT_SOURCE = "https://github.com/snowflakedb/snowflake-cli-templates"


def _path_argument_callback(path: str) -> str:
    if SecurePath(path).exists():
        raise ClickException(
            f"The directory {path} already exists. Please specify a different path for the project."
        )
    return path


PathArgument = typer.Argument(
    ...,
    help="Directory to be initialized with the project. This directory must not already exist",
    show_default=False,
    callback=_path_argument_callback,
)
TemplateOption = typer.Option(
    None,
    "--template",
    help="which template (subdirectory of --template-source) should be used. If not provided,"
    " whole source will be used as the template.",
    show_default=False,
)
SourceOption = typer.Option(
    default=DEFAULT_SOURCE,
    help=f"local path to template directory or URL to git repository with templates.",
)

TEMPLATE_METADATA_FILE_NAME = "template.yml"


def _fetch_local_template(
    template_source: SecurePath, path: Optional[str], destination: SecurePath
) -> SecurePath:
    """Copies local template to [dest] and returns path to the template root.
    Ends with an error of the template does not exist."""

    template_source.assert_exists()
    template_origin = template_source / path if path else template_source
    if not template_origin.exists():
        raise ClickException(
            f"Template '{path}' cannot be found under {template_source}"
        )

    template_origin.copy(destination.path)
    return destination / template_origin.name


def _fetch_remote_template(
    url: str, path: Optional[str], destination: SecurePath
) -> SecurePath:
    """Downloads remote repository template to [dest],
    and returns path to the template root.
    Ends with an error of the template does not exist."""
    from git import rmtree as git_rmtree

    # TODO: during nativeapp refactor get rid of this dependency
    from snowflake.cli.plugins.nativeapp.utils import shallow_git_clone

    shallow_git_clone(url, to_path=destination.path)
    if path:
        template_root = destination / path
    else:
        # remove .git directory not to copy it to the template
        template_root = destination
        git_rmtree((template_root / ".git").path)
    if not template_root.exists():
        raise ClickException(f"Template '{path}' cannot be found under {url}")

    return template_root


def _read_template_metadata(template_root: SecurePath) -> Template:
    """Parse template.yml file."""
    template_metadata_path = template_root / TEMPLATE_METADATA_FILE_NAME
    if not template_metadata_path.exists():
        raise FileNotFoundError(
            f"Template does not have {TEMPLATE_METADATA_FILE_NAME} file"
        )
    with template_metadata_path.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
        yaml_contents = yaml.safe_load(fd) or {}
    return Template(template_root, **yaml_contents)


def _remove_template_metadata_file(template_root: SecurePath) -> None:
    (template_root / TEMPLATE_METADATA_FILE_NAME).unlink()


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
    """
    Prompt user for values not provided in [variables_from_flags].
    If [no_interactive] is True, fill not provided variables with their default values.
    """
    result = {}

    for variable in variables_metadata:
        if variable.name not in variables_from_flags:
            value = _prompt_for_value(variable, no_interactive)
        else:
            value = variables_from_flags[variable.name]
            if variable.type:
                value = variable.type.python_type(value)

        result[variable.name] = value

    return result


def _render_template(
    template_root: SecurePath, files_to_render: List[str], data: Dict[str, Any]
) -> None:
    """Override all listed files with their rendered version."""
    jinja_env = get_template_cli_jinja_env(template_root)
    for path in files_to_render:
        jinja_template = jinja_env.get_template(path)
        rendered_result = jinja_template.render(**data)
        full_path = template_root / path
        full_path.write_text(rendered_result)


def _validate_cli_version(required_version: str) -> None:
    from packaging.version import parse
    from snowflake.cli.__about__ import VERSION

    if parse(required_version) > parse(VERSION):
        raise ClickException(
            f"Snowflake CLI version ({VERSION}) is too low - minimum version required"
            f" by template is {required_version}. Please upgrade before continuing."
        )


@app.command(no_args_is_help=True)
def init(
    path: str = PathArgument,
    template: Optional[str] = TemplateOption,
    template_source: Optional[str] = SourceOption,
    variables: Optional[List[str]] = VariablesOption,
    no_interactive: bool = NoInteractiveOption,
    **options,
) -> CommandResult:
    """
    Creates project from template.
    """
    variables_from_flags = {
        v.key: v.value for v in parse_key_value_variables(variables)
    }
    is_remote = any(
        template_source.startswith(prefix) for prefix in ["git@", "http://", "https://"]  # type: ignore
    )

    # copy/download template into tmpdir, so it is going to be removed in case command ends with an error
    with SecurePath.temporary_directory() as tmpdir:
        if is_remote:
            template_root = _fetch_remote_template(
                url=template_source, path=template, destination=tmpdir  # type: ignore
            )
        else:
            template_root = _fetch_local_template(
                template_source=SecurePath(template_source),
                path=template,
                destination=tmpdir,
            )

        template_metadata = _read_template_metadata(template_root)
        if template_metadata.minimum_cli_version:
            _validate_cli_version(template_metadata.minimum_cli_version)

        variable_values = _determine_variable_values(
            variables_metadata=template_metadata.variables,
            variables_from_flags=variables_from_flags,
            no_interactive=no_interactive,
        )
        _render_template(
            template_root=template_root,
            files_to_render=template_metadata.files_to_render,
            data=variable_values,
        )
        _remove_template_metadata_file(template_root)
        template_root.copy(path)

    return MessageResult(f"Initialized the new project in {path}")
