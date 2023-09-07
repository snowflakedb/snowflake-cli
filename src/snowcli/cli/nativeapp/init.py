from __future__ import annotations

import logging
import sys
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from typer import Abort

from typing import Optional
from snowcli.cli.project.definition import DEFAULT_USERNAME
from snowcli.cli.project.util import clean_identifier, get_env_username
from snowcli.cli.render.commands import generic_render_template
from snowcli.utils import get_client_git_version


log = logging.getLogger(__name__)

SNOWFLAKELABS_GITHUB_URL = "https://github.com/Snowflake-Labs/native-app-templates"
BASIC_TEMPLATE = "native-app-basic"


class InitError(Exception):
    """
    Native app project could not be initiated due to an underlying error.
    """

    def __str__(self):
        return self.__doc__


class GitVersionIncompatibleError(InitError):
    """
    Init requires git version to be at least 2.25.0. Please update git and try again.
    """

    pass


class GitCloneError(InitError):
    """
    Could not complete git clone with the specified git repository URL.
    """

    pass


class RenderingFromJinjaError(InitError):
    """
    Could not complete rendering file from Jinja template.
    """

    message: str | None

    def __init__(self, message: str | None = None):
        self.message = message

    def __str__(self):
        return self.message + f"\n{self.__doc__}"


class CannotInitializeAnExistingProjectError(InitError):
    """
    Cannot initialize a new project within an existing Native Application project.
    """

    def __str__(self):
        return self.__doc__


class DirectoryAlreadyExistsError(InitError):
    """
    Directory already contains a project with the intended name
    """

    name: str

    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __str__(self):
        return f"This directory already contains a sub-directory called {self.name}. Please try a different name."


def _sparse_checkout(
    git_url: str, repo_sub_directory: str, target_parent_directory: str
):
    """
    Clone the requested sub directory of a git repository from the provided git url.

    Args:
        git_url (str): The full git url to the repository to be cloned.
        repo_sub_directory (str): The sub directory name within the repository to be cloned.
        target_parent_directory (str): The parent directory where the git repository will be cloned into.

    Returns:
        None
    """

    clone_command = (
        f"git clone -n --depth=1 --filter=tree:0 {git_url} {target_parent_directory}"
    )
    sparse_checkout_command = f"""
        cd {target_parent_directory} &&
            git sparse-checkout set --no-cone {repo_sub_directory} &&
                git checkout
        """
    try:
        subprocess.run(
            clone_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        subprocess.run(
            sparse_checkout_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except subprocess.CalledProcessError as err:
        log.error(err.stderr)
        raise GitCloneError()


def _move_and_rename_project(
    source_parent_directory: Path,
    target_parent_directory: Path,
    repo_sub_directory: str,
    new_name: str,
):
    """
    Move the newly cloned repository's sub directory from its source directory to target directory, and rename to new_name.

    Args:
        source_parent_directory (Path): The source parent directory of the sub directory.
        target_parent_directory (Path): The target parent directory for the sub directory.
        repo_sub_directory (str): The sub directory name within the cloned repository.
        new_name (str): The new name to give to the sub directory after moving.

    Returns:
        None
    """

    # Move to target parent directory
    source_path = source_parent_directory.joinpath(repo_sub_directory)
    source_path.rename(target_parent_directory / source_path.name)

    # Rename directory
    old_name = target_parent_directory.joinpath(repo_sub_directory)
    old_name.rename(old_name.parent / new_name)


def render_snowflake_yml(parent_to_snowflake_yml: Path):
    """
    Create a snowflake.yml file from a jinja template at a given path.

    Args:
        parent_to_snowflake_yml (Path): The parent directory of snowflake.yml.jinja, and later snowflake.yml

    Returns:
        None
    """

    snowflake_yml_jinja = "snowflake.yml.jinja"

    try:
        generic_render_template(
            template_path=parent_to_snowflake_yml.joinpath(snowflake_yml_jinja),
            data={"project_name": parent_to_snowflake_yml.name},
            output_file_path=parent_to_snowflake_yml.joinpath("snowflake.yml"),
        )
        subprocess.run(
            f"rm {snowflake_yml_jinja}",
            shell=True,
            cwd=str(parent_to_snowflake_yml),
        )
    except Exception as err:
        log.error(err)
        raise RenderingFromJinjaError(
            "Error rendering snowflake.yml from snowflake.yml.jinja."
        )


def render_nativeapp_readme(parent_to_readme: Path, project_name: str):
    """
    Create a README.yml file from a jinja template at a given path.

    Args:
        parent_to_readme (Path): The parent directory of README.md.jinja, and later README.md

    Returns:
        None
    """

    readme_jinja = "README.md.jinja"

    default_application_name_prefix = clean_identifier(project_name)
    default_application_name_suffix = clean_identifier(
        get_env_username() or DEFAULT_USERNAME
    )

    try:
        generic_render_template(
            template_path=parent_to_readme.joinpath(readme_jinja),
            data={
                "application_name": f"{default_application_name_prefix}_{default_application_name_suffix}"
            },
            output_file_path=parent_to_readme.joinpath("README.md"),
        )
        subprocess.run(
            f"rm {readme_jinja}",
            shell=True,
            cwd=str(parent_to_readme),
        )
    except Exception as err:
        log.error(err)
        raise RenderingFromJinjaError("Error rendering README.md from readme.md.jinja.")


def _init_without_user_provided_template(
    current_working_directory: Path, project_name: str
):
    """
    Initialize a Native Apps project without any template specified by the user.

    Args:
        current_working_directory (str): The current working directory of the user where the project will be added.
        project_name (str): Name of the project to be created.

    Returns:
        None
    """

    try:
        with TemporaryDirectory(dir=current_working_directory) as temp_dir:
            # Checkout the basic template, which will now reside at ./native-apps-templates/native-app-basic
            _sparse_checkout(
                git_url=SNOWFLAKELABS_GITHUB_URL,
                repo_sub_directory=BASIC_TEMPLATE,
                target_parent_directory=temp_dir,
            )

            # Move native-app-basic to current_working_directory and rename to name
            _move_and_rename_project(
                source_parent_directory=current_working_directory.joinpath(temp_dir),
                target_parent_directory=current_working_directory,
                repo_sub_directory=BASIC_TEMPLATE,
                new_name=project_name,
            )

        # Render snowflake.yml file from its jinja template
        render_snowflake_yml(
            parent_to_snowflake_yml=current_working_directory.joinpath(project_name)
        )

        # Render README.md file from its jinja template
        render_nativeapp_readme(
            parent_to_readme=current_working_directory.joinpath(project_name, "app"),
            project_name=project_name,
        )

    except InitError as err:
        log.error(err)
        raise InitError()


def nativeapp_init(name: str, template: Optional[str] = None):
    """
    Initialize a Native Apps project in the user's local directory, with or without the use of a template.
    """

    current_working_directory = Path.cwd()

    # If current directory is already contains a file named snowflake.yml, i.e. is a native apps project, fail init command.
    # We do not validate the yml here though.
    path_to_snowflake_yml = current_working_directory.joinpath("snowflake.yml")
    if path_to_snowflake_yml.is_file():
        raise CannotInitializeAnExistingProjectError()

    # If a subdirectory with the same name as name exists in the current directory, fail init command
    path_to_project = current_working_directory.joinpath(name)
    if path_to_project.exists():
        raise DirectoryAlreadyExistsError(name)

    if template:  # If user provided a template, use the template
        # Implementation to be added as part of https://snowflakecomputing.atlassian.net/browse/SNOW-896905
        pass
    else:  # No template provided, use Native Apps Basic Template
        # The logic makes use of git sparse checkout, which was introduced in git 2.25.0. Check client's installed git version.
        if get_client_git_version() < "2.25":
            raise GitVersionIncompatibleError()
        _init_without_user_provided_template(
            current_working_directory=current_working_directory,
            project_name=name,
        )
