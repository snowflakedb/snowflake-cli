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

from functools import cached_property
from pathlib import Path
from typing import List

from snowflake.cli.api.project.definition import default_app_package
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.util import to_identifier
from snowflake.cli.plugins.nativeapp.artifacts import resolve_without_follow


class NativeAppProject:
    """
    Represents properties of a native app project.
    """

    def __init__(self, project_definition: NativeApp, project_root: Path):
        self._project_definition = project_definition
        self._project_root = resolve_without_follow(project_root)

    @property
    def project_root(self) -> Path:
        return self._project_root

    @property
    def definition(self) -> NativeApp:
        return self._project_definition

    @cached_property
    def project_identifier(self) -> str:
        # name is expected to be a valid Snowflake identifier, but PyYAML
        # will sometimes strip out double quotes so we try to get them back here.
        return to_identifier(self.definition.name)


class NativeAppPackage:
    """
    Represents information related to a native app package entity as part of a Snowflake CLI project.
    """

    def __init__(self, project: NativeAppProject):
        self._project = project

    @property
    def project(self) -> NativeAppProject:
        return self._project

    @cached_property
    def artifacts(self) -> List[PathMapping]:
        return self.project.definition.artifacts

    @cached_property
    def bundle_root(self) -> Path:
        return Path(self.project.project_root, self.project.definition.bundle_root)

    @cached_property
    def deploy_root(self) -> Path:
        return Path(self.project.project_root, self.project.definition.deploy_root)

    @cached_property
    def generated_root(self) -> Path:
        return Path(self.deploy_root, self.project.definition.generated_root)

    @cached_property
    def package_scripts(self) -> List[str]:
        """
        Relative paths to package scripts from the project root.
        """
        if self.project.definition.package and self.project.definition.package.scripts:
            return self.project.definition.package.scripts
        else:
            return []

    @cached_property
    def package_name(self) -> str:
        if self.project.definition.package and self.project.definition.package.name:
            return to_identifier(self.project.definition.package.name)
        else:
            return to_identifier(default_app_package(self.project.project_identifier))
