# Copyright (c) 2025 Snowflake Inc.
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

from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml
from snowflake.cli._plugins.dbt.constants import PROFILES_FILENAME
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB, ObjectType
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class DBTManager(SqlExecutionMixin):
    def list(self) -> SnowflakeCursor:  # noqa: A003
        query = "SHOW DBT PROJECTS"
        return self.execute_query(query)

    @staticmethod
    def exists(name: FQN) -> bool:
        return ObjectManager().object_exists(
            object_type=ObjectType.DBT_PROJECT.value.cli_name, fqn=name
        )

    def deploy(
        self,
        name: FQN,
        path: SecurePath,
        profiles_path: SecurePath,
        force: bool,
    ) -> SnowflakeCursor:
        dbt_project_path = path / "dbt_project.yml"
        if not dbt_project_path.exists():
            raise CliError(
                f"dbt_project.yml does not exist in directory {path.path.absolute()}."
            )

        with dbt_project_path.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
            dbt_project = yaml.safe_load(fd)
            try:
                profile = dbt_project["profile"]
            except KeyError:
                raise CliError("`profile` is not defined in dbt_project.yml")

        self._validate_profiles(profiles_path, profile)

        with cli_console.phase("Creating temporary stage"):
            stage_manager = StageManager()
            stage_fqn = FQN.from_string(f"dbt_{name}_stage").using_context()
            stage_name = stage_manager.get_standard_stage_prefix(stage_fqn)
            stage_manager.create(stage_fqn, temporary=True)

        with cli_console.phase("Copying project files to stage"):
            with TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                stage_manager.copy_to_tmp_dir(path.path, tmp_path)
                self._prepare_profiles_file(profiles_path.path, tmp_path)
                result_count = len(
                    list(
                        stage_manager.put_recursive(
                            path.path, stage_name, temp_directory=tmp_path
                        )
                    )
                )
                cli_console.step(f"Copied {result_count} files")

        with cli_console.phase("Creating DBT project"):
            if force is True:
                query = f"CREATE OR REPLACE DBT PROJECT {name}"
            elif self.exists(name=name):
                query = f"ALTER DBT PROJECT {name} ADD VERSION"
            else:
                query = f"CREATE DBT PROJECT {name}"
            query += f"\nFROM {stage_name}"
            return self.execute_query(query)

    @staticmethod
    def _validate_profiles(profiles_path: SecurePath, target_profile: str) -> None:
        """
        Validates that:
         * profiles.yml exists
         * contain profile specified in dbt_project.yml
         * no other profiles are defined there
         * does not contain any confidential data like passwords
        """
        profiles_file = profiles_path / PROFILES_FILENAME
        if not profiles_file.exists():
            raise CliError(
                f"{PROFILES_FILENAME} does not exist in directory {profiles_path.path.absolute()}."
            )
        with profiles_file.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
            profiles = yaml.safe_load(fd)

        if target_profile not in profiles:
            raise CliError(
                f"profile {target_profile} is not defined in {PROFILES_FILENAME}"
            )

        errors = defaultdict(list)
        if len(profiles.keys()) > 1:
            for profile_name in profiles.keys():
                if profile_name.lower() != target_profile.lower():
                    errors[profile_name].append("Remove unnecessary profiles")

        required_fields = {
            "account",
            "database",
            "role",
            "schema",
            "type",
            "user",
            "warehouse",
        }
        supported_fields = {
            "threads",
        }
        for target_name, target in profiles[target_profile]["outputs"].items():
            if missing_keys := required_fields - set(target.keys()):
                errors[target_profile].append(
                    f"Missing required fields: {', '.join(sorted(missing_keys))} in target {target_name}"
                )
            if (
                unsupported_keys := set(target.keys())
                - required_fields
                - supported_fields
            ):
                errors[target_profile].append(
                    f"Unsupported fields found: {', '.join(sorted(unsupported_keys))} in target {target_name}"
                )
            if "type" in target and target["type"].lower() != "snowflake":
                errors[target_profile].append(
                    f"Value for type field is invalid. Should be set to `snowflake` in target {target_name}"
                )

        if errors:
            message = f"Found following errors in {PROFILES_FILENAME}. Please fix them before proceeding:"
            for target, issues in errors.items():
                message += f"\n{target}"
                message += "\n * " + "\n * ".join(issues)
            raise CliError(message)

    @staticmethod
    def _prepare_profiles_file(profiles_path: Path, tmp_path: Path):
        # We need to copy profiles.yml file (not symlink) in order to redact
        # any comments without changing original file. This can be achieved
        # with pyyaml, which looses comments while reading a yaml file
        source_profiles_file = SecurePath(profiles_path / PROFILES_FILENAME)
        target_profiles_file = SecurePath(tmp_path / PROFILES_FILENAME)
        if target_profiles_file.exists():
            target_profiles_file.unlink()
        with source_profiles_file.open(
            read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB
        ) as sfd, target_profiles_file.open(mode="w") as tfd:
            yaml.safe_dump(yaml.safe_load(sfd), tfd)

    def execute(
        self, dbt_command: str, name: str, run_async: bool, *dbt_cli_args
    ) -> SnowflakeCursor:
        if dbt_cli_args:
            dbt_command = " ".join([dbt_command, *dbt_cli_args]).strip()
        query = f"EXECUTE DBT PROJECT {name} args='{dbt_command}'"
        return self.execute_query(query, _exec_async=run_async)
