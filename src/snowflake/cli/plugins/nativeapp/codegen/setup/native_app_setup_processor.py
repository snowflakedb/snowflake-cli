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

import os
import subprocess
from pathlib import Path
from typing import Any, Optional
from venv import EnvBuilder

from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    ProcessorMapping,
)
from snowflake.cli.plugins.nativeapp.artifacts import BundleMap
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
    is_python_file_artifact,
)
from snowflake.cli.plugins.nativeapp.project_model import NativeAppProjectModel

_SELF_DIR = Path(__file__).parent


class SandboxEnvBuilder(EnvBuilder):
    def __init__(self, path: Path, **kwargs) -> None:
        super().__init__(**kwargs)
        self._path = path
        self._context = None
        self.create(self._path)

    def run_python(self, *args):
        positional_args = [
            self._context.env_exe,
            "-E",
            *args,
        ]  # passing -E ignores all PYTHON* env vars
        kwargs = {}
        env = dict(os.environ)
        env["VIRTUAL_ENV"] = self._context.env_dir
        kwargs["cwd"] = self._context.env_dir
        subprocess.check_output(positional_args, **kwargs)

    def pip_install(self, *args: Any) -> None:
        self.run_python("-m", "pip", "install", *[str(arg) for arg in args])

    def post_setup(self, context):
        self._context = context


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
        bundle_map.add(artifact_to_process)

        self._create_or_update_sandbox()

        cc.phase("Processing Python setup files")

        for src_file, dest_file in bundle_map.all_mappings(
            absolute=True, expand_directories=True, predicate=is_python_file_artifact
        ):
            cc.step(f"Would process {src_file} -> {dest_file}")

    def _create_or_update_sandbox(self):
        bundle_root = self._na_project.bundle_root
        sandbox_root = bundle_root / "_setup_py_venv"
        cc.step(
            f"Creating virtual environment in {sandbox_root.relative_to(self._na_project.project_root)}"
        )
        sandbox_root.mkdir(exist_ok=True, parents=True)

        # TODO: in this early stage we always clear the virtual env, but this needs to be optimized
        # before the feature is released.
        env_builder = SandboxEnvBuilder(sandbox_root, with_pip=True, clear=True)

        # Install the snowflake.app library in the sandbox
        # Temporarily, we fetch this from the CLI project directory
        lib_path = _SELF_DIR / "snowflake-app-python"
        env_builder.pip_install(lib_path)
