from __future__ import annotations

import os
from pathlib import Path
from typing import List
import functools
from typing import Optional
from snowcli.exception import MissingConfiguration
from snowcli.cli.project.definition import load_project_definition


class DefinitionManager:
    BASE_CONFIG_FILENAME = "snowflake.yml"
    USER_CONFIG_FILENAME = "snowflake.local.yml"

    project_root: Path
    _project_config_paths: List[Path]

    def __init__(self, project: str | None = None) -> None:
        search_path = os.getcwd()
        if project and len(project) > 0:
            search_path = os.path.abspath(project)
        config_files = self._find_config_files(Path(search_path))
        if not config_files:
            raise MissingConfiguration(
                f"Cannot find native app project configuration. Please provide a path to the project or run this command in a valid native app project directory."
            )
        self._project_config_paths = config_files

    def _find_config_files(self, project_path: Path) -> Optional[List[Path]]:
        parent_path = project_path
        starting_mount = project_path.is_mount()
        while parent_path:
            if (
                project_path.is_mount() != starting_mount
                or parent_path.parent == parent_path
                or parent_path == Path.home()
            ):
                return None

            base_config_file_path = self._base_config_file_if_available(parent_path)
            if base_config_file_path:
                user_config_file_path = self._user_config_file_if_available(parent_path)
                self.project_root = parent_path
                if user_config_file_path:
                    return [
                        base_config_file_path,
                        user_config_file_path,
                    ]
                return [base_config_file_path]

            parent_path = parent_path.parent
        return None

    def _config_if_available(
        self, config_filename, project_path: Path
    ) -> Optional[Path]:
        config_file_path = Path(project_path) / config_filename
        if config_file_path.is_file():
            return config_file_path
        return None

    def _base_config_file_if_available(self, project_path: Path) -> Optional[Path]:
        return self._config_if_available(self.BASE_CONFIG_FILENAME, project_path)

    def _user_config_file_if_available(self, project_path: Path) -> Optional[Path]:
        return self._config_if_available(self.USER_CONFIG_FILENAME, project_path)

    @functools.cached_property
    def project_definition(self) -> dict:
        return load_project_definition(self._project_config_paths)
