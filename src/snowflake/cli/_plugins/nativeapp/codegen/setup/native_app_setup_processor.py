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
import logging
import os.path
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

log = logging.getLogger(__name__)


def safe_set(d: dict, *keys: str, **kwargs) -> None:
    """
    Sets a value in a nested dictionary structure, creating intermediate dictionaries as needed.
    Sample usage:

      d = {}
      safe_set(d, "a", "b", "c", value=42)

    d is now:
      {
        "a": {
          "b": {
            "c": 42
          }
        }
      }
    """
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

        cc.step("Processing Python setup files")

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
            log.debug(msg)

        warnings = result.get("warnings", [])
        for msg in warnings:
            cc.warning(msg)

        schema_version = result.get("schema_version")
        if schema_version != "1":
            raise ClickException(
                f"Unsupported schema version returned from snowflake-app-python library: {schema_version}"
            )

        setup_script_mods = [
            mod
            for mod in result.get("modifications", [])
            if mod.get("target") == "native_app:setup_script"
        ]
        if setup_script_mods:
            self._edit_setup_sql(setup_script_mods)

        manifest_mods = [
            mod
            for mod in result.get("modifications", [])
            if mod.get("target") == "native_app:manifest"
        ]
        if manifest_mods:
            self._edit_manifest(manifest_mods)

    def _execute_in_sandbox(self, py_files: List[Path]) -> dict:
        file_count = len(py_files)
        cc.step(f"Processing {file_count} setup file{'s' if file_count > 1 else ''}")

        manifest_path = find_manifest_file(deploy_root=self._bundle_ctx.deploy_root)

        generated_root = self._bundle_ctx.generated_root
        generated_root.mkdir(exist_ok=True, parents=True)

        env_vars = {
            "_SNOWFLAKE_CLI_PROJECT_PATH": str(self._bundle_ctx.project_root),
            "_SNOWFLAKE_CLI_SETUP_FILES": os.pathsep.join(map(str, py_files)),
            "_SNOWFLAKE_CLI_APP_NAME": str(self._bundle_ctx.package_name),
            "_SNOWFLAKE_CLI_SQL_DEST_DIR": str(generated_root),
            "_SNOWFLAKE_CLI_MANIFEST_PATH": str(manifest_path),
        }

        try:
            result = execute_script_in_sandbox(
                script_source=DRIVER_PATH.read_text(),
                env_type=ExecutionEnvironmentType.VENV,
                cwd=self._bundle_ctx.bundle_root,
                timeout=DEFAULT_TIMEOUT,
                path=self.sandbox_root,
                env_vars=env_vars,
            )
        except Exception as e:
            raise ClickException(
                f"Exception while executing python setup script logic: {e}"
            )

        if result.returncode == 0:
            return json.loads(result.stdout)
        else:
            raise ClickException(
                f"Failed to execute python setup script logic: {result.stderr}"
            )

    def _edit_setup_sql(self, modifications: List[dict]) -> None:
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
                            appended.append(self._setup_mod_instruction_to_sql(inst))
                        elif default_loc == "start":
                            prepended.append(self._setup_mod_instruction_to_sql(inst))

            if prepended or appended:
                f.edited_contents = "\n".join(prepended + [f.contents] + appended)

    def _edit_manifest(self, modifications: List[dict]) -> None:
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

    def _setup_mod_instruction_to_sql(self, mod_inst: dict) -> str:
        payload = mod_inst.get("payload")
        if not payload:
            raise ClickException("Unsupported instruction received: no payload found")

        payload_type = payload.get("type")
        if payload_type == "execute immediate":
            file_path = payload.get("file_path")
            if file_path:
                sql_file_path = self._bundle_ctx.generated_root / file_path
                return f"EXECUTE IMMEDIATE FROM '/{to_stage_path(sql_file_path.relative_to(self._bundle_ctx.deploy_root))}';"

        raise ClickException(f"Unsupported instruction type received: {payload_type}")

    @property
    def sandbox_root(self):
        return self._bundle_ctx.bundle_root / "venv"

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
