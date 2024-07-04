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
from pathlib import Path
from textwrap import dedent
from typing import Any, Optional, Union

from click import ClickException
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    ProcessorMapping,
)
from snowflake.cli.api.project.util import to_identifier, to_string_literal
from snowflake.cli.api.utils.rendering import jinja_render_from_file
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
TEMPLATE_PATH = Path(__file__).parent / "setup_driver.py.jinja"


def _generate_primitive_value_sql(value: Any) -> str:
    if value is True:
        return "TRUE"
    if value is False:
        return "FALSE"
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return to_string_literal(value)
    return str(value)


def _generate_create_compute_pool(compute_pool: dict) -> str:
    sql_text = dedent(
        f"""
        CREATE COMPUTE POOL IF NOT EXISTS {to_identifier(compute_pool['name'])}
    """.strip()
    )

    for key in sorted(compute_pool.keys()):
        if key == "name":
            continue
        sql_text += (
            f"\n  {key.upper()} = {_generate_primitive_value_sql(compute_pool[key])}"
        )
    sql_text += ";"
    return sql_text


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
            cc.step(f"Processing {src_file}")
            self._execute_in_sandbox(src_file)

    def _execute_in_sandbox(self, py_file: Union[str, Path]) -> None:
        # Create the code snippet to be executed in the sandbox
        script_source = jinja_render_from_file(
            template_path=TEMPLATE_PATH,
            data={
                "py_file": str(py_file),
                "project_root": str(self._na_project.project_root),
            },
        )

        try:
            result = execute_script_in_sandbox(
                script_source=script_source,
                env_type=ExecutionEnvironmentType.VENV,
                cwd=self._na_project.bundle_root,
                timeout=DEFAULT_TIMEOUT,
                path=self._sandbox_root(),
            )
        except Exception as e:
            raise ClickException(
                f"Exception while executing python setup script logic: {e}"
            )

        if result.returncode == 0:
            collected = json.loads(result.stdout)
            self._generate_setup_sql(collected)
        else:
            raise ClickException(
                f"Failed to execute python setup script logic: {result.stderr}"
            )

    def _generate_setup_sql(self, collected: dict) -> None:
        if not collected:
            # Nothing to generate
            return

        generated_root = self._na_project.generated_root
        generated_root.mkdir(exist_ok=True, parents=True)
        sql_file = generated_root / "__snowflake_py_setup.sql"
        if sql_file.exists():
            raise ClickException(
                f"Naming conflict, {generated_root.relative_to(self._na_project.deploy_root)} already exists"
            )

        with sql_file.open("w") as f:
            f.write("-- This was was generated by the Snowflake CLI\n")
            f.write("-- DO NOT EDIT\n")
            f.write(self._generate_sql(collected))

        cc.step("Patching setup script")
        setup_file_path = find_setup_script_file(
            deploy_root=self._na_project.deploy_root
        )
        with self.edit_file(setup_file_path) as f:
            file_to_load = sql_file.relative_to(self._na_project.deploy_root)
            f.edited_contents = (
                f.contents
                + "\n"
                + f"EXECUTE IMMEDIATE FROM '/{to_stage_path(file_to_load)}';"
            )

    def _generate_sql(self, collected: dict) -> str:
        lines = []

        if "compute_pools" in collected:
            lines.append("")
            lines.append("-- Compute pools --")
            for compute_pool in collected["compute_pools"]:
                lines.append(_generate_create_compute_pool(compute_pool))
            lines.append("")

        return "\n".join(lines)

    def _sandbox_root(self):
        return self._na_project.bundle_root / "_setup_py_venv"

    def _create_or_update_sandbox(self):
        sandbox_root = self._sandbox_root()
        env_builder = SandboxEnvBuilder(sandbox_root, with_pip=True)
        if sandbox_root.exists():
            cc.step("Virtual environment found")
        else:
            cc.step(
                f"Creating virtual environment in {sandbox_root.relative_to(self._na_project.project_root)}"
            )
        env_builder.ensure_created()

        # Install the snowflake.app library in the sandbox
        # Temporarily, we fetch this from a fixed path
        lib_path = Path("~/workplace/snowflake-app-python").expanduser()
        env_builder.pip_install(lib_path)
