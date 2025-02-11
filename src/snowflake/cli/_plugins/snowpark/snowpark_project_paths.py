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

import glob
import os
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Optional

from snowflake.cli._plugins.snowpark.zipper import zip_dir
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import DEPLOYMENT_STAGE
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.project_paths import ProjectPaths, bundle_root
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.secure_path import SecurePath


@dataclass
class SnowparkProjectPaths(ProjectPaths):
    """
    This class allows you to manage files paths related to given project.
    """

    def path_relative_to_root(self, artifact_path: Path) -> Path:
        if artifact_path.is_absolute():
            return artifact_path
        return (self.project_root / artifact_path).resolve()

    def get_artifact_dto(self, artifact_path: PathMapping) -> Artifact:
        if FeatureFlag.ENABLE_SNOWPARK_GLOB_SUPPORT.is_enabled():
            return Artifact(
                project_root=self.project_root,
                bundle_root=self.bundle_root,
                dest=artifact_path.dest,
                path=Path(artifact_path.src),
            )
        else:
            return ArtifactOldBuild(
                dest=artifact_path.dest,
                path=self.path_relative_to_root(Path(artifact_path.src)),
            )

    def get_dependencies_artifact(self) -> Artifact:
        if FeatureFlag.ENABLE_SNOWPARK_GLOB_SUPPORT.is_enabled():
            return Artifact(
                project_root=self.project_root,
                bundle_root=self.bundle_root,
                dest=None,
                path=Path("dependencies.zip"),
            )
        else:
            return ArtifactOldBuild(
                dest=None, path=self.path_relative_to_root(Path("dependencies.zip"))
            )

    @property
    def snowflake_requirements(self) -> SecurePath:
        return SecurePath(
            self.path_relative_to_root(Path("requirements.snowflake.txt"))
        )

    @property
    def requirements(self) -> SecurePath:
        return SecurePath(self.path_relative_to_root(Path("requirements.txt")))

    @property
    def bundle_root(self) -> Path:
        return bundle_root(self.project_root, "snowpark")


@dataclass(unsafe_hash=True)
class Artifact:
    """Helper for getting paths related to given artifact."""

    project_root: Path
    bundle_root: Path
    path: Path
    dest: str | None = None

    def __init__(
        self,
        project_root: Path,
        bundle_root: Path,
        path: Path,
        dest: Optional[str] = None,
    ) -> None:
        self.project_root = project_root
        self.bundle_root = bundle_root
        self.path = path
        self.dest = dest
        if self.dest and not self._is_dest_a_file() and not self.dest.endswith("/"):
            self.dest = self.dest + "/"

    @property
    def _artifact_name(self) -> str:
        """
        Returns artifact name. Directories are mapped to corresponding .zip files.
        For paths with glob patterns, the last part of the path is used.
        For files, the file name is used.
        """
        if glob.has_magic(str(self.path)):
            last_part = None
            for part in self.path.parts:
                if glob.has_magic(part):
                    break
                else:
                    last_part = part
            if not last_part:
                last_part = os.path.commonpath(
                    [str(self.path), str(self.path.absolute())]
                )
            return last_part + ".zip"
        if (self.project_root / self.path).is_dir():
            return self.path.stem + ".zip"
        if (self.project_root / self.path).is_file() and self._is_dest_a_file():
            return Path(self.dest).name  # type: ignore
        return self.path.name

    @property
    def post_build_path(self) -> Path:
        """
        Returns post-build artifact path. Directories are mapped to corresponding .zip files.
        """
        bundle_root = self.bundle_root
        path = (
            self._path_until_asterisk()
            if glob.has_magic(str(self.path))
            else self.path.parent
        )
        if self._is_dest_a_file():
            return bundle_root / self.dest  # type: ignore
        return bundle_root / (self.dest or path) / self._artifact_name

    def upload_path(self, stage: FQN | str | None) -> str:
        """
        Path on stage to which the artifact should be uploaded.
        """
        stage = stage or DEPLOYMENT_STAGE
        if isinstance(stage, str):
            stage = FQN.from_stage(stage).using_context()

        stage_path = PurePosixPath(f"@{stage}")
        if self.dest:
            stage_path /= (
                PurePosixPath(self.dest).parent if self._is_dest_a_file() else self.dest
            )
        else:
            stage_path /= (
                self._path_until_asterisk()
                if glob.has_magic(str(self.path))
                else PurePosixPath(self.path).parent
            )

        return str(stage_path) + "/"

    def import_path(self, stage: FQN | str | None) -> str:
        """Path for UDF/sproc imports clause."""
        return self.upload_path(stage) + self._artifact_name

    def _is_dest_a_file(self) -> bool:
        if not self.dest:
            return False
        return re.search(r"\.[a-zA-Z0-9]{2,4}$", self.dest) is not None

    def _path_until_asterisk(self) -> Path:
        path = []
        for part in self.path.parts:
            if glob.has_magic(part):
                break
            else:
                path.append(part)
        return Path(*path[:-1])

    # Can be removed after removing ENABLE_SNOWPARK_GLOB_SUPPORT feature flag.
    def build(self) -> None:
        raise NotImplementedError("Not implemented in Artifact class.")


@dataclass(unsafe_hash=True)
class ArtifactOldBuild(Artifact):
    """Helper for getting paths related to given artifact."""

    path: Path
    dest: str | None = None

    def __init__(self, path: Path, dest: Optional[str] = None) -> None:
        super().__init__(project_root=Path(), bundle_root=Path(), path=path, dest=dest)

    @property
    def _artifact_name(self) -> str:
        if self.path.is_dir():
            return self.path.stem + ".zip"
        return self.path.name

    @property
    def post_build_path(self) -> Path:
        """
        Returns post-build artifact path. Directories are mapped to corresponding .zip files.
        """
        return self.path.parent / self._artifact_name

    def upload_path(self, stage: FQN | str | None) -> str:
        """
        Path on stage to which the artifact should be uploaded.
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
        return self.upload_path(stage) + self._artifact_name

    def build(self) -> None:
        """Build the artifact. Applies only to directories. Files are untouched."""
        if not self.path.is_dir():
            return
        cli_console.step(f"Creating: {self.post_build_path.name}")
        zip_dir(
            source=self.path,
            dest_zip=self.post_build_path,
        )
