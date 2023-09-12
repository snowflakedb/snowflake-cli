from __future__ import annotations

import os
from pathlib import Path
from typing import List
import functools
from typing import Optional
from snowcli.exception import MissingConfiguration
from snowcli.cli.project.definition import load_project_definition


class DefinitionManager:
    BASE_DEFINITION_FILENAME = "snowflake.yml"
    USER_DEFINITION_FILENAME = "snowflake.local.yml"

    project_root: Path
    _project_config_paths: List[Path]

    def __init__(self, project_arg: Optional[str] = None) -> None:
        search_path = os.getcwd()
        if project_arg:
            search_path = os.path.abspath(project_arg)
        config_files = self._find_definition_files(Path(search_path))
        if not config_files:
            raise MissingConfiguration(
                f"Cannot find project definition (snowflake.yml). Please provide a path to the project or run this command in a valid project directory."
            )
        self._project_config_paths = config_files

    def _find_definition_files(self, project_path: Path) -> Optional[List[Path]]:
        """
        Recurse up the directory tree from the given search path until we find
        a directory that contains a snowflake.yml file. We'll stop if we cross
        a filesystem boundary or hit the user's HOME directory.
        """
        parent_path = project_path
        starting_mount = project_path.is_mount()
        while parent_path:
            if (
                project_path.is_mount() != starting_mount
                or parent_path.parent == parent_path
                or parent_path == Path.home()
            ):
                return None

            base_config_file_path = self._base_definition_file_if_available(parent_path)
            if base_config_file_path:
                user_config_file_path = self._user_definition_file_if_available(
                    parent_path
                )
                self.project_root = parent_path
                if user_config_file_path:
                    return [
                        base_config_file_path,
                        user_config_file_path,
                    ]
                return [base_config_file_path]

            parent_path = parent_path.parent
        return None

    def _definition_if_available(self, filename, project_path: Path) -> Optional[Path]:
        file_path = Path(project_path) / filename
        if file_path.is_file():
            return file_path
        return None

    def _base_definition_file_if_available(self, project_path: Path) -> Optional[Path]:
        return self._definition_if_available(
            self.BASE_DEFINITION_FILENAME, project_path
        )

    def _user_definition_file_if_available(self, project_path: Path) -> Optional[Path]:
        return self._definition_if_available(
            self.USER_DEFINITION_FILENAME, project_path
        )

    @functools.cached_property
    def project_definition(self) -> dict:
        return load_project_definition(self._project_config_paths)
