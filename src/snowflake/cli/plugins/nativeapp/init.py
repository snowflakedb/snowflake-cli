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
import re
from pathlib import Path
from typing import Optional

from click.exceptions import ClickException
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import MissingConfiguration
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.util import (
    is_valid_identifier,
    is_valid_unquoted_identifier,
    to_identifier,
)
from snowflake.cli.api.rendering.jinja import jinja_render_from_file
from snowflake.cli.api.secure_path import SecurePath
from yaml import dump, safe_dump, safe_load

log = logging.getLogger(__name__)

OFFICIAL_TEMPLATES_GITHUB_URL = "https://github.com/snowflakedb/native-apps-templates"
BASIC_TEMPLATE = "basic"


class InitError(ClickException):
    """
    Snowflake Native App project could not be initiated due to an underlying error.
    """

    def __init__(self):
        super().__init__(self.__doc__)


class ProjectNameInvalidError(ClickException):
    """
    Intended project name does not qualify as a valid identifier.
    """

    def __init__(self, project_name: str):
        super().__init__(
            f"Intended project name does not qualify as a valid identifier: {project_name}"
        )


class RenderingFromJinjaError(ClickException):
    """
    Could not complete rendering file from Jinja template.
    """

    def __init__(self, name: str):
        super().__init__(
            f"Could not complete rendering file from Jinja template: {name}"
        )


class CannotInitializeAnExistingProjectError(ClickException):
    """
    Cannot initialize a new project within an existing Snowflake Native App project.
    """

    def __init__(self):
        super().__init__(self.__doc__)


class DirectoryAlreadyExistsError(ClickException):
    """
    Directory already contains a project with the intended name.
    """

    name: str

    def __init__(self, name: str):
        super().__init__(
            f"The directory {name} already exists. Please specify a different path for the project."
        )
        self.name = name


class TemplateNotFoundError(ClickException):
    """
    Specified template was not found.
    """

    def __init__(self, template_name):
        super().__init__(f"Specified template was not found: {template_name}")


class ProjectDescriptor:
    """
    Encapsulates static properties of a Snowflake Native App project.
    """

    def __init__(self, *, name, path):
        self.name = name
        self.path = path


def _to_yaml_string(identifier: str):
    """
    Returns the YAML representation of an identifier, suitable for including in a YAML jinja template
    """
    if is_valid_unquoted_identifier(identifier):
        return identifier
    else:
        return dump(identifier).rstrip()


def _render_snowflake_yml(parent_to_snowflake_yml: Path, project_identifier: str):
    """
    Create a snowflake.yml file from a jinja template at a given path.

    Args:
        parent_to_snowflake_yml (Path): The parent directory of snowflake.yml.jinja, and later snowflake.yml
        project_identifier (str): The name of the project to be created, as a Snowflake identifier.

    Returns:
        None
    """

    snowflake_yml_jinja = "snowflake.yml.jinja"

    try:
        jinja_render_from_file(
            template_path=parent_to_snowflake_yml / snowflake_yml_jinja,
            data={
                # generic_render_template operates on text, not YAML, so escape before rendering
                "project_name": _to_yaml_string(project_identifier)
            },
            output_file_path=parent_to_snowflake_yml / "snowflake.yml",
        )
        (parent_to_snowflake_yml / snowflake_yml_jinja).unlink()
    except Exception as err:
        log.error(err)
        raise RenderingFromJinjaError(snowflake_yml_jinja)


def _replace_snowflake_yml_name_with_project(
    target_directory: Path, project_identifier: str
):
    """
    Replace the native_app schema's "name" field in a snowflake.yml file with its parent directory name, i.e. the Snowflake Native App project, as the default start.
    This does not change the name in any other snowflake.*.yml as snowflake.yml is the base file and all others are overrides for the user to customize.

    Args:
        target_directory (str): The directory containing snowflake.yml at its root.
        project_identifier (str): The name of the project to be created, as a Snowflake identifier.

    Returns:
        None
    """

    path_to_snowflake_yml = SecurePath(target_directory) / "snowflake.yml"
    contents = None

    with path_to_snowflake_yml.open(
        "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB
    ) as file:
        contents = safe_load(file)

    if (
        (contents is not None)
        and ("native_app" in contents)
        and ("name" in contents["native_app"])
        and (contents["native_app"]["name"] != project_identifier)
    ):
        contents["native_app"]["name"] = project_identifier
        with path_to_snowflake_yml.open("w") as file:
            safe_dump(contents, file, sort_keys=False)


def _validate_and_update_snowflake_yml(target_directory: Path, project_identifier: str):
    """
    Update the native_app name key in the snowflake.yml file and perform validation on the entire file.
    This step is useful when cloning from a non-Snowflake template repo which may directly have a snowflake.yml file.

    Args:
        target_directory (Path): The directory containing snowflake.yml at its root.
        project_identifier (str): The name of the project to be created, as a Snowflake identifier.

    Returns:
        None
    """
    # 1. Determine if a snowflake.yml file exists, at the very least
    definition_manager = DefinitionManager(target_directory)
    if not definition_manager.has_definition_file:
        raise MissingConfiguration(
            "Cannot find project definition (snowflake.yml). Please provide a path to the project or run this command in a valid project directory."
        )

    # 2. Change the project name in snowflake.yml if necessary
    _replace_snowflake_yml_name_with_project(
        target_directory=target_directory, project_identifier=project_identifier
    )

    # 3. Validate the Project Definition File(s)
    definition_manager.project_definition


def _generate_project_name_from_path(p: Path):
    return re.sub(r"[. -]+", "_", p.name)


def _init_from_template(
    project_path: Path,
    project_identifier: str,
    git_url: Optional[str],
    template: Optional[str],
):
    """
    Initialize a Snowflake Native App project with a git URL and optionally a specific template within the git URL.

    Args:
        project_path (Path): The directory of the user where the project will be added.
            project_identifier (str): The name of the project to be created, as a Snowflake identifier.
        git_url (str): The git URL to perform a clone from.
        template (str): A optional template within the git URL to use, all other directories and files outside the
            template will be discarded.

    Returns:
        None
    """
    use_whole_repo_as_template = git_url and not template
    if not use_whole_repo_as_template:
        git_url = git_url if git_url else OFFICIAL_TEMPLATES_GITHUB_URL

    try:
        with SecurePath.temporary_directory() as temp_path:
            from git import rmtree as git_rmtree
            from snowflake.cli.plugins.nativeapp.utils import shallow_git_clone

            shallow_git_clone(git_url, temp_path.path)

            if use_whole_repo_as_template:
                # the template is the entire git repository
                template_root = temp_path
                # Remove all git history before we move the repo
                git_rmtree((template_root / ".git").path)
            else:
                # The template is a subdirectory of the git repository
                template_name = template if template else BASIC_TEMPLATE
                template_root = temp_path / template_name
                if not template_root.path.is_dir():
                    raise TemplateNotFoundError(template_name=template_name)

            if (template_root / "snowflake.yml.jinja").exists():
                # Render snowflake.yml file from its jinja template
                _render_snowflake_yml(
                    parent_to_snowflake_yml=template_root.path,
                    project_identifier=project_identifier,
                )

            # If not an official Snowflake Native App template
            if git_url != OFFICIAL_TEMPLATES_GITHUB_URL:
                _validate_and_update_snowflake_yml(
                    target_directory=template_root.path,
                    project_identifier=project_identifier,
                )

            project_path.parent.mkdir(parents=True, exist_ok=True)

            # Move the template to the specified path
            template_root.move(project_path)

    except TemplateNotFoundError:
        raise
    except Exception as err:
        # If there was any error, validation on Project Definition file or otherwise,
        # there should not be any Snowflake Native App Project left after this.
        if project_path.exists():
            SecurePath(project_path).rmdir(recursive=True)

        log.error(err)
        raise InitError()


def nativeapp_init(
    path: str,
    name: Optional[str] = None,
    git_url: Optional[str] = None,
    template: Optional[str] = None,
) -> ProjectDescriptor:
    """
    Initialize a Snowflake Native App project in the user's current working directory, with or without the use of a template.

    Args:
        path (str): The location of the project to be created.
        name (str): Name of the project to be created.
        git_url (str): The git URL to perform a clone from.
        template (str): A template within the git URL to use, all other directories and files outside the template will be discarded.

    Returns:
        A project descriptor for the newly initialized project.
    """
    try:
        project_path = Path(path).expanduser().resolve()
    except Exception as err:  # expanduser can fail
        raise InitError()

    # If a subdirectory with the same name as name exists in the current directory, fail init command
    if project_path.exists():
        raise DirectoryAlreadyExistsError(path)

    # Check if the specified path already exists in a Snowflake Native App project. Nesting projects is not allowed.
    if DefinitionManager.find_project_root(search_path=project_path.parent) is not None:
        raise CannotInitializeAnExistingProjectError()

    project_name = (
        name if name is not None else _generate_project_name_from_path(project_path)
    )
    if not project_name:
        # empty name
        raise ProjectNameInvalidError(project_name=project_name)

    if not is_valid_identifier(project_name) and (
        project_name.startswith('"') or project_name.endswith('"')
    ):
        # the project name looks like it was partially quoted. This is likely a mistake, reject it rather than
        # silently escaping it.
        raise ProjectNameInvalidError(project_name=project_name)

    project_identifier = to_identifier(project_name)
    _init_from_template(
        project_path=project_path,
        project_identifier=project_identifier,
        git_url=git_url,
        template=template,
    )

    return ProjectDescriptor(name=project_name, path=project_path)
