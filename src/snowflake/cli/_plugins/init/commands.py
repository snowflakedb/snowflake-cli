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

import logging
from typing import Any, Dict, List, Optional

import typer
import yaml
from click import ClickException
from snowflake.cli.__about__ import VERSION
from snowflake.cli.api.commands.flags import (
    NoInteractiveOption,
    variables_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import InvalidTemplateError
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
)
from snowflake.cli.api.project.schemas.template import Template, TemplateVariable
from snowflake.cli.api.rendering.project_templates import render_template_files
from snowflake.cli.api.secure_path import SecurePath

# simple Typer with defaults because it won't become a command group as it contains only one command
app = SnowTyperFactory()


DEFAULT_SOURCE = "https://github.com/snowflakedb/snowflake-cli-templates"

log = logging.getLogger(__name__)


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
    DEFAULT_SOURCE,
    "--template-source",
    help=f"local path to template directory or URL to git repository with templates.",
)
VariablesOption = variables_option(
    "String in `key=value` format. Provided variables will not be prompted for."
)

TEMPLATE_METADATA_FILE_NAME = "template.yml"


def _fetch_local_template(
    template_source: SecurePath, path: Optional[str], destination: SecurePath
) -> SecurePath:
    """Copies local template to [dest] and returns path to the template root.
    Ends with an error of the template does not exist."""

    template_source.assert_exists()
    template_origin = template_source / path if path else template_source
    log.info("Copying local template from %s", template_origin.path)
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
    from git import GitCommandError
    from git import rmtree as git_rmtree

    # TODO: during nativeapp refactor get rid of this dependency
    from snowflake.cli._plugins.nativeapp.utils import shallow_git_clone

    log.info("Downloading remote template from %s", url)
    try:
        shallow_git_clone(url, to_path=destination.path)
    except GitCommandError as err:
        import re

        if re.search("fatal: repository '.*' not found", err.stderr):
            raise ClickException(f"Repository '{url}' does not exist")
        raise

    if path:
        # template is a subdirectoruy of the repository
        template_root = destination / path
    else:
        # template is a whole repository
        # removing .git directory not to copy it to the template
        template_root = destination
        git_rmtree((template_root / ".git").path)
    if not template_root.exists():
        raise ClickException(f"Template '{path}' cannot be found under {url}")

    return template_root


def _read_template_metadata(template_root: SecurePath, args_error_msg: str) -> Template:
    """Parse template.yml file."""
    template_metadata_path = template_root / TEMPLATE_METADATA_FILE_NAME
    log.debug("Reading template metadata from %s", template_metadata_path.path)
    if not template_metadata_path.exists():
        raise InvalidTemplateError(
            f"File {TEMPLATE_METADATA_FILE_NAME} not found. {args_error_msg}"
        )
    with template_metadata_path.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
        yaml_contents = yaml.safe_load(fd) or {}
    return Template(template_root, **yaml_contents)


def _remove_template_metadata_file(template_root: SecurePath) -> None:
    (template_root / TEMPLATE_METADATA_FILE_NAME).unlink()


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

    log.debug(
        "Resolving values of variables: %s",
        ", ".join(v.name for v in variables_metadata),
    )
    for variable in variables_metadata:
        if variable.name in variables_from_flags:
            value = variable.python_type(variables_from_flags[variable.name])
        else:
            value = variable.prompt_user_for_value(no_interactive)

        result[variable.name] = value

    return result


def _validate_cli_version(required_version: str) -> None:
    from packaging.version import parse

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
    Creates project directory from template.
    """
    variables_from_flags = {
        v.key: v.value for v in parse_key_value_variables(variables)
    }
    is_remote = template_source is not None and any(
        template_source.startswith(prefix)
        for prefix in ["git@", "http://", "https://"]  # type: ignore
    )
    args_error_msg = f"Check whether {TemplateOption.param_decls[0]} and {SourceOption.param_decls[0]} arguments are correct."

    # copy/download template into tmpdir, so it is going to be removed in case command ends with an error
    with SecurePath.temporary_directory() as tmpdir:
        if is_remote:
            template_root = _fetch_remote_template(
                url=template_source,  # type: ignore
                path=template,
                destination=tmpdir,  # type: ignore
            )
        else:
            template_root = _fetch_local_template(
                template_source=SecurePath(template_source),  # type: ignore
                path=template,
                destination=tmpdir,
            )

        template_metadata = _read_template_metadata(
            template_root, args_error_msg=args_error_msg
        )
        if template_metadata.minimum_cli_version:
            _validate_cli_version(template_metadata.minimum_cli_version)

        variable_values = _determine_variable_values(
            variables_metadata=template_metadata.variables,
            variables_from_flags=variables_from_flags,
            no_interactive=no_interactive,
        ) | {
            "project_dir_name": SecurePath(path).name,
            "snowflake_cli_version": VERSION,
        }
        log.debug(
            "Rendering template files: %s", ", ".join(template_metadata.files_to_render)
        )
        render_template_files(
            template_root=template_root,
            files_to_render=template_metadata.files_to_render,
            data=variable_values,
        )
        _remove_template_metadata_file(template_root)
        SecurePath(path).parent.mkdir(exist_ok=True, parents=True)
        template_root.copy(path)

    return MessageResult(f"Initialized the new project in {path}")
