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

import logging
import os
import re
import shlex
import sys
from dataclasses import dataclass
from pathlib import Path
from subprocess import run as _subprocess_run
from typing import Dict, List, Optional, Tuple

import yaml

from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.project.schemas.project_definition import DefinitionV20
from snowflake.cli.api.project.schemas.scripts import ScriptModel
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils.models import ProjectEnvironment

log = logging.getLogger(__name__)

VARIABLE_PATTERN = re.compile(r"\$\{([^}]+)\}")
MANIFEST_FILE_NAME = "manifest.yml"


@dataclass
class ScriptExecutionResult:
    script_name: str
    exit_code: int
    success: bool


class ScriptManager:
    """Manager for loading and executing project scripts."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self._scripts: Dict[str, ScriptModel] = {}
        self._scripts_source: Optional[str] = None
        self._load_scripts()

    def _load_scripts(self) -> None:
        """Load scripts from snowflake.yml or manifest.yml.

        Scripts can be defined in either file but not both.
        Raises CliError if scripts are found in both files.
        """
        snowflake_scripts, snowflake_source = self._load_snowflake_scripts()
        manifest_scripts, manifest_source = self._load_manifest_scripts()

        if snowflake_scripts and manifest_scripts:
            raise CliError(
                "Scripts defined in both manifest.yml and snowflake.yml.\n"
                "Scripts must be defined in only one file per directory.\n\n"
                "Recommendation: Move all scripts to one file.\n"
                "- Use manifest.yml for DCM-focused projects\n"
                "- Use snowflake.yml for app-focused projects"
            )

        if snowflake_scripts:
            self._scripts = snowflake_scripts
            self._scripts_source = snowflake_source
        elif manifest_scripts:
            self._scripts = manifest_scripts
            self._scripts_source = manifest_source

    def _load_snowflake_scripts(
        self,
    ) -> Tuple[Optional[Dict[str, ScriptModel]], Optional[str]]:
        """Load scripts from snowflake.yml via project definition."""
        ctx = get_cli_context()
        project_def = ctx.project_definition
        if (
            project_def
            and isinstance(project_def, DefinitionV20)
            and project_def.scripts
        ):
            return project_def.scripts, "snowflake.yml"
        return None, None

    def _load_manifest_scripts(
        self,
    ) -> Tuple[Optional[Dict[str, ScriptModel]], Optional[str]]:
        """Load scripts from manifest.yml if present."""
        manifest_path = SecurePath(self.project_root / MANIFEST_FILE_NAME)
        if not manifest_path.exists():
            return None, None

        try:
            with manifest_path.open("r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as f:
                manifest_data = yaml.safe_load(f.read())
        except Exception as e:
            log.debug("Could not read manifest.yml: %s", e)
            return None, None

        if not manifest_data or "scripts" not in manifest_data:
            return None, None

        scripts = self._parse_manifest_scripts(manifest_data["scripts"])
        return scripts, "manifest.yml"

    def _parse_manifest_scripts(self, scripts_data: dict) -> Dict[str, ScriptModel]:
        """Parse scripts section from manifest.yml into ScriptModel objects."""
        result = {}
        for name, script_def in scripts_data.items():
            if isinstance(script_def, str):
                result[name] = ScriptModel(cmd=script_def)
            else:
                result[name] = ScriptModel(**script_def)
        return result

    @property
    def scripts_source(self) -> Optional[str]:
        """Return the source file for scripts (snowflake.yml or manifest.yml)."""
        return self._scripts_source

    def list_scripts(self) -> Dict[str, ScriptModel]:
        """Return all available scripts."""
        return self._scripts

    def get_script(self, name: str) -> Optional[ScriptModel]:
        """Get a script by name."""
        return self._scripts.get(name)

    def _build_variable_context(
        self, var_overrides: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """Build the context dict for variable interpolation."""
        ctx = get_cli_context()
        template_context = ctx.template_context.get("ctx", {})

        variables: Dict[str, str] = {}

        defaults = template_context.get("defaults", {})
        if defaults:
            for key in ["database", "schema", "connection"]:
                if key in defaults and defaults[key]:
                    variables[key] = str(defaults[key])

        env_section = template_context.get("env")
        if env_section is not None:
            if isinstance(env_section, ProjectEnvironment):
                if env_section.default_env:
                    for key, value in env_section.default_env.items():
                        variables[f"env.{key}"] = str(value)
            elif isinstance(env_section, dict):
                for key, value in env_section.items():
                    variables[f"env.{key}"] = str(value)

        for key, value in os.environ.items():
            env_key = f"env.{key}"
            if env_key not in variables:
                variables[env_key] = value

        entities = template_context.get("entities", {})
        if entities:
            for entity_name, entity_data in entities.items():
                if isinstance(entity_data, dict):
                    self._flatten_entity(
                        f"entity.{entity_name}", entity_data, variables
                    )

        if var_overrides:
            variables.update(var_overrides)

        return variables

    def _flatten_entity(
        self, prefix: str, data: dict, variables: Dict[str, str]
    ) -> None:
        """Flatten nested entity data into dot-notation keys."""
        for key, value in data.items():
            full_key = f"{prefix}.{key}"
            if isinstance(value, dict):
                self._flatten_entity(full_key, value, variables)
            elif value is not None:
                variables[full_key] = str(value)

    def interpolate_variables(
        self,
        cmd: str,
        var_overrides: Optional[Dict[str, str]] = None,
        shell_mode: bool = False,
    ) -> str:
        """Interpolate variables in command string."""
        variables = self._build_variable_context(var_overrides)

        def replace_var(match: re.Match) -> str:
            var_name = match.group(1)
            if var_name in variables:
                value = variables[var_name]
                if shell_mode:
                    return shlex.quote(value)
                return value
            raise CliError(
                f"Undefined variable '${{{var_name}}}' in script command. "
                f"Define it in your project env section or pass --var {var_name}=VALUE"
            )

        return VARIABLE_PATTERN.sub(replace_var, cmd)

    def execute_script(
        self,
        name: str,
        extra_args: Optional[List[str]] = None,
        var_overrides: Optional[Dict[str, str]] = None,
        dry_run: bool = False,
        verbose: bool = False,
        continue_on_error: bool = False,
        _call_stack: Optional[List[str]] = None,
    ) -> ScriptExecutionResult:
        """Execute a script by name."""
        if _call_stack is None:
            _call_stack = []

        if name in _call_stack:
            raise CliError(
                f"Circular dependency detected: {' -> '.join(_call_stack)} -> {name}"
            )

        script = self.get_script(name)
        if not script:
            raise ValueError(f"Script '{name}' not found")

        if script.run:
            return self._execute_composite(
                name,
                script,
                extra_args,
                var_overrides,
                dry_run,
                verbose,
                continue_on_error,
                _call_stack=_call_stack + [name],
            )

        return self._execute_command(
            name, script, extra_args, var_overrides, dry_run, verbose
        )

    def _execute_command(
        self,
        name: str,
        script: ScriptModel,
        extra_args: Optional[List[str]],
        var_overrides: Optional[Dict[str, str]],
        dry_run: bool,
        verbose: bool,
    ) -> ScriptExecutionResult:
        """Execute a single command script."""
        cmd = self.interpolate_variables(
            script.cmd, var_overrides, shell_mode=bool(script.shell)
        )

        if extra_args:
            cmd = f"{cmd} {' '.join(shlex.quote(arg) for arg in extra_args)}"

        cc.message(f"Running script: {name}")
        cc.message(f"> {cmd}")

        if dry_run:
            return ScriptExecutionResult(name, 0, True)

        cwd = self.project_root
        if script.cwd:
            cwd = self.project_root / script.cwd

        env = os.environ.copy()
        if script.env:
            env.update(script.env)

        if script.shell:
            if sys.platform == "win32":
                result = _subprocess_run(
                    cmd,
                    shell=True,
                    cwd=cwd,
                    env=env,
                )
            else:
                result = _subprocess_run(
                    cmd,
                    shell=True,
                    cwd=cwd,
                    env=env,
                    executable="/bin/sh",
                )
        else:
            args = shlex.split(cmd)
            result = _subprocess_run(
                args,
                cwd=cwd,
                env=env,
            )

        return ScriptExecutionResult(name, result.returncode, result.returncode == 0)

    def _execute_composite(
        self,
        name: str,
        script: ScriptModel,
        extra_args: Optional[List[str]],
        var_overrides: Optional[Dict[str, str]],
        dry_run: bool,
        verbose: bool,
        continue_on_error: bool,
        _call_stack: Optional[List[str]] = None,
    ) -> ScriptExecutionResult:
        """Execute a composite script (list of scripts)."""
        if _call_stack is None:
            _call_stack = []

        cc.message(f"Running script: {name}")

        total = len(script.run)
        failed_scripts = []
        first_failure_exit_code = 1

        for idx, sub_name in enumerate(script.run, 1):
            cc.message(f"\n[{idx}/{total}] {sub_name}")

            if sub_name not in self._scripts:
                cc.warning(f"Script '{sub_name}' not found, skipping")
                if not continue_on_error:
                    return ScriptExecutionResult(name, 1, False)
                failed_scripts.append(sub_name)
                continue

            result = self.execute_script(
                sub_name,
                extra_args=None,
                var_overrides=var_overrides,
                dry_run=dry_run,
                verbose=verbose,
                continue_on_error=continue_on_error,
                _call_stack=_call_stack,
            )

            if not result.success:
                if not failed_scripts:
                    first_failure_exit_code = result.exit_code
                if not continue_on_error:
                    return result
                failed_scripts.append(sub_name)

        if failed_scripts:
            cc.warning(f"\nCompleted with errors in: {', '.join(failed_scripts)}")
            return ScriptExecutionResult(name, first_failure_exit_code, False)

        cc.message(f"\nDone! ({total} scripts executed)")
        return ScriptExecutionResult(name, 0, True)
