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

from pathlib import Path
from typing import Optional

from click import ClickException
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    ProcessorMapping,
)
from snowflake.cli.plugins.nativeapp.artifacts import BundleMap
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import ArtifactProcessor
from snowflake.cli.plugins.nativeapp.project_model import NativeAppProjectModel


def _is_python_file_artifact(src: Path, dest: Path):
    return src.is_file() and src.suffix == ".py"


class NativeAppSetupProcessor(ArtifactProcessor):
    def __init__(
        self,
        na_project: NativeAppProjectModel,
    ):
        super().__init__(na_project=na_project)

    def process(
        self,
        artifact_to_process: PathMapping,
        processor_mapping: Optional[ProcessorMapping],
        **kwargs,
    ) -> None:
        """
        Processes a Python setup script and generated the corresponding SQL commands.
        """
        bundle_map = BundleMap(
            project_root=self._na_project.project_root,
            deploy_root=self._na_project.deploy_root,
        )
        if artifact_to_process.dest is not None:
            raise ClickException(
                f"Python setup artifact must not have a destination, src={artifact_to_process.src}"
            )
        bundle_map.add(artifact_to_process)

        self._create_or_update_sandbox()

        cc.phase("Processing Python setup files")

        for src_file, dest_file in bundle_map.all_mappings(
            absolute=True, expand_directories=True, predicate=_is_python_file_artifact
        ):
            cc.step(f"Would process {src_file} -> {dest_file}")

    def _create_or_update_sandbox(self):
        cc.step(f"Would create sandbox in {self._na_project.bundle_root}")
