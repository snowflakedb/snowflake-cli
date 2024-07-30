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

import filecmp
import json
import os.path
import shutil
from pathlib import Path
from typing import List, Optional

import yaml
from click import ClickException
from snowflake.cli._plugins.nativeapp.artifacts import (
    BundleMap,
    find_manifest_file,
    find_setup_script_file,
)
from snowflake.cli._plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
    is_python_file_artifact,
)
from snowflake.cli._plugins.nativeapp.codegen.sandbox import (
    ExecutionEnvironmentType,
    SandboxEnvBuilder,
    execute_script_in_sandbox,
)
from snowflake.cli._plugins.stage.diff import to_stage_path
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    ProcessorMapping,
)

DEFAULT_TIMEOUT = 30
DRIVER_PATH = Path(__file__).parent / "setup_driver.py.source"


def safe_set(d: dict, *keys: str, **kwargs) -> None:
    curr = d
    for k in keys[:-1]:
        curr = curr.setdefault(k, {})

    curr[keys[-1]] = kwargs.get("value")


class NativeAppSetupProcessor(ArtifactProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
            project_root=self._bundle_ctx.project_root,
            deploy_root=self._bundle_ctx.deploy_root,
        )
        bundle_map.add(artifact_to_process)

        self._create_or_update_sandbox()

        cc.phase("Processing Python setup files")

        files_to_process = []
        for src_file, dest_file in bundle_map.all_mappings(
            absolute=True, expand_directories=True, predicate=is_python_file_artifact
        ):
            cc.message(
                f"Found Python setup file: {src_file.relative_to(self._bundle_ctx.project_root)}"
            )
            files_to_process.append(src_file)

        result = self._execute_in_sandbox(files_to_process)
        if not result:
            return  # nothing to do

        logs = result.get("logs", [])
        for msg in logs:
            cc.message(f"LOG: {msg}")

        warnings = result.get("warnings", [])
        for msg in warnings:
            cc.warning(msg)

        if result.get("schema_version") == "1":
            setup_script_mods = [
                mod
                for mod in result.get("modifications", [])
                if mod.get("target") == "native_app:setup_script"
            ]
            self._edit_setup_sql(setup_script_mods)

            manifest_mods = [
                mod
                for mod in result.get("modifications", [])
                if mod.get("target") == "native_app:manifest"
            ]
            self._edit_manifest(manifest_mods)
        else:
            self._generate_setup_sql_legacy(result)

    def _execute_in_sandbox(self, py_files: List[Path]) -> dict:
        file_count = len(py_files)
        cc.step(f"Processing {file_count} setup file{'s' if file_count > 1 else ''}")

        manifest_path = find_manifest_file(deploy_root=self._bundle_ctx.deploy_root)
        temp_manifest_path = self.bundle_root / manifest_path.name
        shutil.copyfile(manifest_path, temp_manifest_path)

        env_vars = {
            "_SNOWFLAKE_CLI_PROJECT_PATH": str(self._bundle_ctx.project_root),
            "_SNOWFLAKE_CLI_SETUP_FILES": os.pathsep.join(map(str, py_files)),
            "_SNOWFLAKE_CLI_APP_NAME": str(self._bundle_ctx.package_name),
            "_SNOWFLAKE_CLI_SQL_DEST_DIR": str(self.generated_root),
            "_SNOWFLAKE_CLI_MANIFEST_PATH": str(temp_manifest_path),
        }

        try:
            result = execute_script_in_sandbox(
                script_source=DRIVER_PATH.read_text(),
                env_type=ExecutionEnvironmentType.VENV,
                cwd=self.bundle_root,
                timeout=DEFAULT_TIMEOUT,
                path=self.sandbox_root,
                env_vars=env_vars,
            )
        except Exception as e:
            raise ClickException(
                f"Exception while executing python setup script logic: {e}"
            )

        if result.returncode == 0:
            processor_result = json.loads(result.stdout)

            if not filecmp.cmp(manifest_path, temp_manifest_path):
                # manifest was edited, update the original in the deploy root
                with self.edit_file(manifest_path) as f:
                    f.edited_contents = temp_manifest_path.read_text()

            return processor_result
        else:
            raise ClickException(
                f"Failed to execute python setup script logic: {result.stderr}"
            )

    def _edit_setup_sql(self, modifications: List[dict]) -> None:
        generated_root = self.generated_root
        generated_root.mkdir(exist_ok=True, parents=True)

        if not modifications:
            return

        cc.step("Patching setup script")
        setup_file_path = find_setup_script_file(
            deploy_root=self._bundle_ctx.deploy_root
        )

        with self.edit_file(setup_file_path) as f:
            prepended = []
            appended = []

            for mod in modifications:
                for inst in mod.get("instructions", []):
                    if inst.get("type") == "insert":
                        default_loc = inst.get("default_location")
                        if default_loc == "end":
                            appended.append(self._setup_mod_inst_to_sql(inst))
                        elif default_loc == "start":
                            prepended.append(self._setup_mod_inst_to_sql(inst))

            if prepended or appended:
                f.edited_contents = "\n".join(prepended + [f.contents] + appended)

    def _edit_manifest(self, modifications: List[dict]) -> None:
        if not modifications:
            return

        cc.step("Patching manifest")
        manifest_path = find_manifest_file(deploy_root=self._bundle_ctx.deploy_root)

        with self.edit_file(manifest_path) as f:
            manifest = yaml.safe_load(f.contents)

            for mod in modifications:
                for inst in mod.get("instructions", []):
                    if inst.get("type") == "set":
                        payload = inst.get("payload")
                        if payload:
                            key = payload.get("key")
                            value = payload.get("value")
                            safe_set(manifest, *key.split("."), value=value)
            f.edited_contents = yaml.safe_dump(manifest, sort_keys=False)

    def _setup_mod_inst_to_sql(self, mod_inst: dict) -> str:
        payload = mod_inst["payload"]
        if payload["type"] == "execute immediate":
            file_path = payload.get("file_path")
            if file_path:
                sql_file_path = self.generated_root / file_path
                return f"EXECUTE IMMEDIATE FROM '/{to_stage_path(sql_file_path.relative_to(self._bundle_ctx.deploy_root))}';"

        raise ClickException("Invalid instructions received")

    def _generate_setup_sql_legacy(self, result: dict):
        generated_root = self.generated_root
        generated_root.mkdir(exist_ok=True, parents=True)

        cc.step("Patching setup script")
        setup_file_path = find_setup_script_file(
            deploy_root=self._bundle_ctx.deploy_root
        )

        with self.edit_file(setup_file_path) as f:
            new_contents = [f.contents]

            if result["prepend"]:
                for sql_file in result["prepend"]:
                    sql_file_path = generated_root / sql_file
                    new_contents.insert(
                        0,
                        f"EXECUTE IMMEDIATE FROM '/{to_stage_path(sql_file_path.relative_to(self._bundle_ctx.deploy_root))}';",
                    )

            if result["append"]:
                for sql_file in result["append"]:
                    sql_file_path = generated_root / sql_file
                    new_contents.append(
                        f"EXECUTE IMMEDIATE FROM '/{to_stage_path(sql_file_path.relative_to(self._bundle_ctx.deploy_root))}';",
                    )

            if len(new_contents) > 1:
                f.edited_contents = "\n".join(new_contents)

    @property
    def sandbox_root(self):
        return self.bundle_root / "venv"

    @property
    def generated_root(self):
        return self._bundle_ctx.generated_root / "setup_py"

    @property
    def bundle_root(self):
        return self._bundle_ctx.bundle_root / "setup_py"

    def _create_or_update_sandbox(self):
        sandbox_root = self.sandbox_root
        env_builder = SandboxEnvBuilder(sandbox_root, with_pip=True)
        if sandbox_root.exists():
            cc.step("Virtual environment found")
        else:
            cc.step(
                f"Creating virtual environment in {sandbox_root.relative_to(self._bundle_ctx.project_root)}"
            )
        env_builder.ensure_created()

        # Temporarily fetch the library from a location specified via env vars
        env_builder.pip_install(os.environ["SNOWFLAKE_APP_PYTHON_LOC"])
