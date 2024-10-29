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

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Optional

from snowflake.cli._plugins.snowpark.snowpark_entity_model import PathMapping
from snowflake.cli._plugins.snowpark.zipper import zip_dir
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import DEPLOYMENT_STAGE
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
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
        if FeatureFlag.ENABLE_SNOWPARK_NEW_BUILD.is_enabled():
            return Artefact(
                project_root=self.project_root,
                dest=artifact_path.dest,
                path=Path(artifact_path.src),
            )
        else:
            return ArtefactOldBuild(
                dest=artifact_path.dest,
                path=self.path_relative_to_root(Path(artifact_path.src)),
            )

    def get_dependencies_artefact(self) -> Artefact:
        if FeatureFlag.ENABLE_SNOWPARK_NEW_BUILD.is_enabled():
            return Artefact(
                project_root=self.project_root, dest=None, path=self.dependencies
            )
        else:
            return ArtefactOldBuild(dest=None, path=self.dependencies)

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
        if FeatureFlag.ENABLE_SNOWPARK_NEW_BUILD.is_enabled():
            return Path("dependencies.zip")
        else:
            return self.path_relative_to_root(Path("dependencies.zip"))

    @property
    def deploy_root(self) -> Path:
        return self.project_root / "output"


@dataclass(unsafe_hash=True)
class Artefact:
    """Helper for getting paths related to given artefact."""

    project_root: Path
    path: Path
    dest: str | None = None

    def __init__(
        self, project_root: Path, path: Path, dest: Optional[str] = None
    ) -> None:
        self.project_root = project_root
        self.path = path
        self.dest = dest
        if self.dest and not self.is_dest_a_file() and not self.dest.endswith("/"):
            self.dest = self.dest + "/"

    @property
    def _artefact_name(self) -> str:
        if "*" in str(self.path):
            before_wildcard = str(self.path).split("*")[0]
            last_part = Path(before_wildcard).absolute().parts[-1]
            return last_part + ".zip"
        elif (self.project_root / self.path).is_dir():
            return self.path.stem + ".zip"
        elif (self.project_root / self.path).is_file():
            if self.is_dest_a_file():
                return Path(self.dest).name  # type: ignore
        return self.path.name

    @property
    def post_build_path(self) -> Path:
        """
        Returns post-build artefact path. Directories are mapped to corresponding .zip files.
        """
        deploy_root = self.deploy_root()
        path = self._path_until_asterix() if "*" in str(self.path) else self.path.parent
        return deploy_root / (self.dest or path) / self._artefact_name

    def upload_path(self, stage: FQN | str | None) -> str:
        """
        Path on stage to which the artefact should be uploaded.
        """
        stage = stage or DEPLOYMENT_STAGE
        if isinstance(stage, str):
            stage = FQN.from_stage(stage).using_context()

        stage_path = PurePosixPath(f"@{stage}")
        if self.dest:
            if self.is_dest_a_file():
                stage_path = stage_path / PurePosixPath(self.dest).parent
            else:
                stage_path = stage_path / self.dest
        else:
            if "*" in str(self.path):
                stage_path = stage_path / self._path_until_asterix()
            else:
                stage_path = stage_path / PurePosixPath(self.path).parent
        return str(stage_path) + "/"

    def import_path(self, stage: FQN | str | None) -> str:
        """Path for UDF/sproc imports clause."""
        return self.upload_path(stage) + self._artefact_name

    def deploy_root(self) -> Path:
        return self.project_root / "output"

    def is_dest_a_file(self) -> bool:
        if not self.dest:
            return False
        return re.search(r"\.[a-zA-Z0-9]{2,4}$", self.dest) is not None

    def _path_until_asterix(self) -> Path:
        before_wildcard = str(self.path).split("*")[0]
        parts = Path(before_wildcard).parts[:-1]
        return Path(*parts)

    # Can be removed after removing ENABLE_SNOWPARK_NEW_BUILD feature flag.
    def build(self) -> None:
        raise NotImplementedError("Not implemented in Artefact class.")


@dataclass(unsafe_hash=True)
class ArtefactOldBuild(Artefact):
    """Helper for getting paths related to given artefact."""

    path: Path
    dest: str | None = None

    def __init__(self, path: Path, dest: Optional[str] = None) -> None:
        super().__init__(project_root=Path(), path=path, dest=dest)

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
