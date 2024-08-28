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

from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from snowflake.cli._plugins.snowpark.zipper import zip_dir
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import DEPLOYMENT_STAGE
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.snowpark_entity import PathMapping
from snowflake.cli.api.secure_path import SecurePath


@dataclass
class SnowparkProjectPaths:
    """
    This class represents allows you to manage files paths related to given project.
    """

    project_root: Path

    def path_relative_to_root(self, artifact_path: Path) -> Path:
        if artifact_path.is_absolute():
            return artifact_path
        return (self.project_root / artifact_path).resolve()

    def get_artefact_dto(self, artifact_path: PathMapping) -> Artefact:
        return Artefact(
            dest=artifact_path.dest,
            path=self.path_relative_to_root(artifact_path.src),
        )

    def get_dependencies_artefact(self) -> Artefact:
        return Artefact(dest=None, path=self.dependencies)

    @property
    def snowflake_requirements(self) -> SecurePath:
        return SecurePath(
            self.path_relative_to_root(Path("requirements.snowflake.txt"))
        )

    @property
    def requirements(self) -> SecurePath:
        return SecurePath(self.path_relative_to_root(Path("requirements.txt")))

    @property
    def dependencies(self) -> Path:
        return self.path_relative_to_root(Path("dependencies.zip"))


@dataclass(unsafe_hash=True)
class Artefact:
    """Helper for getting paths related to given artefact."""

    path: Path
    dest: str | None = None

    @property
    def _artefact_name(self) -> str:
        if self.path.is_dir():
            return self.path.stem + ".zip"
        return self.path.name

    @property
    def post_build_path(self) -> Path:
        """
        Returns post-build artefact path. Directories are mapped to corresponding .zip files.
        """
        return self.path.parent / self._artefact_name

    def upload_path(self, stage: FQN | str | None) -> str:
        """
        Path on stage to which the artefact should be uploaded.
        """
        stage = stage or DEPLOYMENT_STAGE
        if isinstance(stage, str):
            stage = FQN.from_stage(stage).using_context()

        stage_path = PurePosixPath(f"@{stage}")
        if self.dest:
            stage_path = stage_path / self.dest
        return str(stage_path) + "/"

    def import_path(self, stage: FQN | str | None) -> str:
        """Path for UDF/sproc imports clause."""
        return self.upload_path(stage) + self._artefact_name

    def build(self) -> None:
        """Build the artefact. Applies only to directories. Files are untouched."""
        if not self.path.is_dir():
            return
        cli_console.step(f"Creating: {self.post_build_path.name}")
        zip_dir(
            source=self.path,
            dest_zip=self.post_build_path,
        )
