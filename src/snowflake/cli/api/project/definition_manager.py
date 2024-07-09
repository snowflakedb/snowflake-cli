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

import functools
import os
from pathlib import Path
from typing import List, Optional

from snowflake.cli.api.project.definition import ProjectProperties, load_project
from snowflake.cli.api.project.schemas.project_definition import ProjectDefinitionV1
from snowflake.cli.api.utils.types import Context


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

    def __init__(
        self,
        project_arg: Optional[str] = None,
        context_overrides: Optional[Context] = None,
    ) -> None:
        project_root = Path(
            os.path.abspath(project_arg) if project_arg else os.getcwd()
        )

        self.project_root = project_root
        self._project_config_paths = self._find_definition_files(self.project_root)
        self._context_overrides = context_overrides

    @functools.cached_property
    def has_definition_file(self):
        return len(self._project_config_paths) > 0

    @staticmethod
    def _find_definition_files(project_root: Path) -> List[Path]:
        base_config_file_path = DefinitionManager._base_definition_file_if_available(
            project_root
        )
        user_config_file_path = DefinitionManager._user_definition_file_if_available(
            project_root
        )

        definition_files: List[Path] = []
        if base_config_file_path:
            definition_files.append(base_config_file_path)
            if user_config_file_path:
                definition_files.append(user_config_file_path)

        return definition_files

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
    def _project_properties(self) -> ProjectProperties:
        return load_project(self._project_config_paths, self._context_overrides)

    @functools.cached_property
    def project_definition(self) -> ProjectDefinitionV1:
        return self._project_properties.project_definition

    @functools.cached_property
    def template_context(self) -> Context:
        return self._project_properties.project_context
