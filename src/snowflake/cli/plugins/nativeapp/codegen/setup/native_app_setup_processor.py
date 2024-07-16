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

import json
import os.path
from pathlib import Path
from typing import List, Optional

from click import ClickException
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    ProcessorMapping,
)
from snowflake.cli.plugins.nativeapp.artifacts import BundleMap, find_setup_script_file
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
    is_python_file_artifact,
)
from snowflake.cli.plugins.nativeapp.codegen.sandbox import (
    ExecutionEnvironmentType,
    SandboxEnvBuilder,
    execute_script_in_sandbox,
)
from snowflake.cli.plugins.nativeapp.project_model import NativeAppProjectModel
from snowflake.cli.plugins.stage.diff import to_stage_path

DEFAULT_TIMEOUT = 30
DRIVER_PATH = Path(__file__).parent / "setup_driver.py.source"


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
        Processes a Python setup script and generates the corresponding SQL commands.
        """
        bundle_map = BundleMap(
            project_root=self._na_project.project_root,
            deploy_root=self._na_project.deploy_root,
        )
        bundle_map.add(artifact_to_process)

        self._create_or_update_sandbox()

        cc.phase("Processing Python setup files")

        files_to_process = []
        for src_file, dest_file in bundle_map.all_mappings(
            absolute=True, expand_directories=True, predicate=is_python_file_artifact
        ):
            cc.message(
                f"Found Python setup file: {src_file.relative_to(self._na_project.project_root)}"
            )
            files_to_process.append(src_file)

        sql_files_mapping = self._execute_in_sandbox(files_to_process)
        self._generate_setup_sql(sql_files_mapping)

    def _execute_in_sandbox(self, py_files: List[Path]) -> dict:
        file_count = len(py_files)
        cc.step(f"Processing {file_count} setup file{'s' if file_count > 1 else ''}")

        env_vars = {
            "_SNOWFLAKE_CLI_PROJECT_PATH": str(self._na_project.project_root),
            "_SNOWFLAKE_CLI_SETUP_FILES": os.pathsep.join(map(str, py_files)),
            "_SNOWFLAKE_CLI_APP_NAME": str(self._na_project.package_name),
            "_SNOWFLAKE_CLI_SQL_DEST_DIR": str(self.generated_root),
        }

        try:
            result = execute_script_in_sandbox(
                script_source=DRIVER_PATH.read_text(),
                env_type=ExecutionEnvironmentType.VENV,
                cwd=self._na_project.bundle_root,
                timeout=DEFAULT_TIMEOUT,
                path=self.sandbox_root,
                env_vars=env_vars,
            )
        except Exception as e:
            raise ClickException(
                f"Exception while executing python setup script logic: {e}"
            )

        if result.returncode == 0:
            sql_file_mappings = json.loads(result.stdout)
            return sql_file_mappings
        else:
            raise ClickException(
                f"Failed to execute python setup script logic: {result.stderr}"
            )

    def _generate_setup_sql(self, sql_file_mappings: dict) -> None:
        if not sql_file_mappings:
            # Nothing to generate
            return

        generated_root = self.generated_root
        generated_root.mkdir(exist_ok=True, parents=True)

        cc.step("Patching setup script")
        setup_file_path = find_setup_script_file(
            deploy_root=self._na_project.deploy_root
        )
        with self.edit_file(setup_file_path) as f:
            new_contents = [f.contents]

            if sql_file_mappings["schemas"]:
                schemas_file = generated_root / sql_file_mappings["schemas"]
                new_contents.insert(
                    0,
                    f"EXECUTE IMMEDIATE FROM '/{to_stage_path(schemas_file.relative_to(self._na_project.deploy_root))}';",
                )

            if sql_file_mappings["compute_pools"]:
                compute_pools_file = generated_root / sql_file_mappings["compute_pools"]
                new_contents.append(
                    f"EXECUTE IMMEDIATE FROM '/{to_stage_path(compute_pools_file.relative_to(self._na_project.deploy_root))}';"
                )

            if sql_file_mappings["services"]:
                services_file = generated_root / sql_file_mappings["services"]
                new_contents.append(
                    f"EXECUTE IMMEDIATE FROM '/{to_stage_path(services_file.relative_to(self._na_project.deploy_root))}';"
                )

            f.edited_contents = "\n".join(new_contents)

    @property
    def sandbox_root(self):
        return self._na_project.bundle_root / "setup_py_venv"

    @property
    def generated_root(self):
        return self._na_project.generated_root / "setup_py"

    def _create_or_update_sandbox(self):
        sandbox_root = self.sandbox_root
        env_builder = SandboxEnvBuilder(sandbox_root, with_pip=True)
        if sandbox_root.exists():
            cc.step("Virtual environment found")
        else:
            cc.step(
                f"Creating virtual environment in {sandbox_root.relative_to(self._na_project.project_root)}"
            )
        env_builder.ensure_created()

        # Temporarily fetch the library from a location specified via env vars
        env_builder.pip_install(os.environ["SNOWFLAKE_APP_PYTHON_LOC"])
