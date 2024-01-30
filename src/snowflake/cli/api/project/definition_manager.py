from __future__ import annotations

import functools
import os
from pathlib import Path
from typing import List, Optional

from snowflake.cli.api.exceptions import MissingConfiguration
from snowflake.cli.api.project.definition import load_project_definition


def _compat_is_mount(path: Path):
    try:
        return path.is_mount()
    except NotImplementedError:
        # If we can't figure out if path is mount then let's assume it is
        # Windows support added in Python 3.12
        return True


class DefinitionManager:
    BASE_DEFINITION_FILENAME = "snowflake.yml"
    USER_DEFINITION_FILENAME = "snowflake.local.yml"

    project_root: Path
    _project_config_paths: List[Path]

    def __init__(self, project_arg: Optional[str] = None) -> None:
        project_root = Path(
            os.path.abspath(project_arg) if project_arg else os.getcwd()
        )
        if not self._base_definition_file_if_available(project_root):
            raise MissingConfiguration(
                f"Cannot find project definition (snowflake.yml). Please provide a path to the project or run this command in a valid project directory."
            )

        self.project_root = project_root
        self._project_config_paths = self._find_definition_files(self.project_root)

    @staticmethod
    def _find_definition_files(project_root: Path) -> List[Path]:
        base_config_file_path = (
            project_root / DefinitionManager.BASE_DEFINITION_FILENAME
        )
        user_config_file_path = DefinitionManager._user_definition_file_if_available(
            project_root
        )
        if user_config_file_path:
            return [
                base_config_file_path,
                user_config_file_path,
            ]
        return [base_config_file_path]

    @staticmethod
    def find_project_root(search_path: Path) -> Optional[Path]:
        """
        Recurse up the directory tree from the given search path until we find
        a directory that contains a snowflake.yml file. We'll stop if we cross
        a filesystem boundary or hit the user's HOME directory.
        """
        current_path = search_path
        starting_mount = _compat_is_mount(search_path)
        while current_path:
            if (
                _compat_is_mount(current_path) != starting_mount
                or current_path.parent == current_path
                or current_path == Path.home()
            ):
                return None

            if (
                DefinitionManager._base_definition_file_if_available(current_path)
                is not None
            ):
                # found snowflake.yml, end the search here
                return current_path

            current_path = current_path.parent

        return None

    @staticmethod
    def _definition_if_available(filename, project_path: Path) -> Optional[Path]:
        file_path = Path(project_path) / filename
        if file_path.is_file():
            return file_path
        return None

    @staticmethod
    def _base_definition_file_if_available(project_path: Path) -> Optional[Path]:
        return DefinitionManager._definition_if_available(
            DefinitionManager.BASE_DEFINITION_FILENAME, project_path
        )

    @staticmethod
    def _user_definition_file_if_available(project_path: Path) -> Optional[Path]:
        return DefinitionManager._definition_if_available(
            DefinitionManager.USER_DEFINITION_FILENAME, project_path
        )

    @functools.cached_property
    def project_definition(self) -> dict:
        return load_project_definition(self._project_config_paths)
