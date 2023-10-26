from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from click.exceptions import ClickException
from shutil import move, rmtree
from strictyaml import load, as_document
from yaml import dump


from typing import Optional
from snowcli.cli.common.utils import generic_render_template
from snowcli.cli.project.definition_manager import DefinitionManager
from snowcli.cli.project.util import (
    is_valid_identifier,
    is_valid_unquoted_identifier,
    to_identifier,
)

log = logging.getLogger(__name__)

OFFICIAL_TEMPLATES_GITHUB_URL = "https://github.com/snowflakedb/native-apps-templates"
BASIC_TEMPLATE = "basic"


class InitError(ClickException):
    """
    Native app project could not be initiated due to an underlying error.
    """

    def __init__(self):
        super().__init__(self.__doc__)


class ProjectNameInvalidError(ClickException):
    """
    Intended project name does not qualify as a valid identifier.
    """

    def __init__(self):
        super().__init__(self.__doc__)


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
    Cannot initialize a new project within an existing Native Application project.
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

    def __init__(self):
        super().__init__(self.__doc__)


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
        generic_render_template(
            template_path=parent_to_snowflake_yml / snowflake_yml_jinja,
            data={
                # generic_render_template operates on text, not YAML, so escape before rendering
                "project_name": _to_yaml_string(project_identifier)
            },
            output_file_path=parent_to_snowflake_yml / "snowflake.yml",
        )
        os.remove(parent_to_snowflake_yml / snowflake_yml_jinja)
    except Exception as err:
        log.error(err)
        raise RenderingFromJinjaError(snowflake_yml_jinja)


def _replace_snowflake_yml_name_with_project(
    target_directory: Path, project_identifier: str
):
    """
    Replace the native_app schema's "name" field in a snowflake.yml file with its parent directory name, i.e. the native app project, as the default start.
    This does not change the name in any other snowflake.*.yml as snowflake.yml is the base file and all others are overrides for the user to customize.

    Args:
        target_directory (str): The directory containing snowflake.yml at its root.
        project_identifier (str): The name of the project to be created, as a Snowflake identifier.

    Returns:
        None
    """

    path_to_snowflake_yml = target_directory / "snowflake.yml"
    contents = None

    with open(path_to_snowflake_yml) as f:
        contents = load(f.read()).data

    if (
        ("native_app" in contents)
        and ("name" in contents["native_app"])
        and (contents["native_app"]["name"] != project_identifier)
    ):
        contents["native_app"]["name"] = project_identifier
        with open(path_to_snowflake_yml, "w") as f:
            f.write(as_document(contents).as_yaml())


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
    Initialize a Native Apps project with a git URL and optionally a specific template within the git URL.

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
        template_name = template if template else BASIC_TEMPLATE

    try:
        with TemporaryDirectory() as temp_dir:
            from git import Repo

            temp_path = Path(temp_dir)

            # Clone the repository in the temporary directory with options.
            Repo.clone_from(
                url=git_url,
                to_path=temp_dir,
                filter=["tree:0"],
                depth=1,
            )

            template_root = (
                temp_path if use_whole_repo_as_template else temp_path / template_name
            )
            if not template_root.is_dir():
                raise TemplateNotFoundError()

            if use_whole_repo_as_template:
                # Remove all git history before we move the repo
                rmtree(template_root.joinpath(".git").resolve())

            if Path.exists(template_root / "snowflake.yml.jinja"):
                # Render snowflake.yml file from its jinja template
                _render_snowflake_yml(
                    parent_to_snowflake_yml=template_root,
                    project_identifier=project_identifier,
                )

            # If not an official Snowflake Native App template
            if git_url != OFFICIAL_TEMPLATES_GITHUB_URL:
                _validate_and_update_snowflake_yml(
                    target_directory=template_root,
                    project_identifier=project_identifier,
                )

            project_path.parent.mkdir(parents=True, exist_ok=True)

            # Move the template to the specified path
            move(
                src=template_root,
                dst=project_path,
            )

    except TemplateNotFoundError:
        raise
    except Exception as err:
        # If there was any error, validation on Project Definition file or otherwise,
        # there should not be any Native Apps Project left after this.
        if project_path.exists():
            rmtree(project_path.resolve())

        log.error(err)
        raise InitError()


def _is_part_of_existing_project(path: Path):
    project_file = path / "snowflake.yml"
    if project_file.is_file():
        # Found a snowflake.yml in an ancestor directory, this path is part of an existing project
        return True

    parent = path.parent
    if parent == path:
        # reached the root without encountering an existing project
        return False

    return _is_part_of_existing_project(parent)


def nativeapp_init(
    path: str,
    name: Optional[str] = None,
    git_url: Optional[str] = None,
    template: Optional[str] = None,
):
    """
    Initialize a Native Apps project in the user's current working directory, with or without the use of a template.

    Args:
        path (str): The location of the project to be created.
        name (str): Name of the project to be created.
        git_url (str): The git URL to perform a clone from.
        template (str): A template within the git URL to use, all other directories and files outside the template will be discarded.

    Returns:
        None
    """
    try:
        project_path = Path(path).expanduser().resolve()
    except Exception as err:  # expanduser can fail
        raise InitError()

    # If a subdirectory with the same name as name exists in the current directory, fail init command
    if project_path.exists():
        raise DirectoryAlreadyExistsError(path)

    # Check if the specified path already exists in a native apps project. Nesting projects is not allowed.
    if _is_part_of_existing_project(project_path.parent):
        raise CannotInitializeAnExistingProjectError()

    project_name = (
        name if name is not None else _generate_project_name_from_path(project_path)
    )
    if not project_name:
        # empty name
        raise ProjectNameInvalidError()

    if not is_valid_identifier(project_name) and (
        project_name.startswith('"') or project_name.endswith('"')
    ):
        # the project name looks like it was partially double quoted. This is likely a mistake, reject it rather than
        # silently escaping it.
        raise ProjectNameInvalidError()

    project_identifier = to_identifier(project_name)
    _init_from_template(
        project_path=project_path,
        project_identifier=project_identifier,
        git_url=git_url,
        template=template,
    )

    return {"name": project_name, "path": project_path}
