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
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List, Optional, TypedDict

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
from snowflake.connector.errors import ProgrammingError


class DBTObjectEditableAttributes(TypedDict):
    default_target: Optional[str]
    external_access_integrations: Optional[List[str]]
    dbt_version: Optional[str]


@dataclass
class DBTDeployAttributes:
    """Attributes for deploying a DBT project."""

    default_target: Optional[str] = None
    unset_default_target: bool = False
    external_access_integrations: Optional[List[str]] = None
    install_local_deps: bool = False
    dbt_version: Optional[str] = None


class DBTManager(SqlExecutionMixin):
    def list(self) -> SnowflakeCursor:  # noqa: A003
        query = "SHOW DBT PROJECTS"
        return self.execute_query(query)

    @staticmethod
    def exists(name: FQN) -> bool:
        return ObjectManager().object_exists(
            object_type=ObjectType.DBT_PROJECT.value.cli_name, fqn=name
        )

    @staticmethod
    def describe(name: FQN) -> SnowflakeCursor:
        return ObjectManager().describe(
            object_type=ObjectType.DBT_PROJECT.value.cli_name, fqn=name
        )

    @staticmethod
    def get_dbt_object_attributes(name: FQN) -> Optional[DBTObjectEditableAttributes]:
        """Get editable attributes of an existing DBT project, or None if it doesn't exist."""
        try:
            cursor = DBTManager().describe(name)
        except ProgrammingError as exc:
            if "DBT PROJECT" in exc.msg and "does not exist" in exc.msg:
                return None
            raise exc

        rows = list(cursor)
        if not rows:
            return None

        row = rows[0]
        # Convert row to dict using column names
        columns = [desc[0].lower() for desc in cursor.description]
        row_dict = dict(zip(columns, row))

        external_access_integrations = row_dict.get("external_access_integrations")
        if external_access_integrations:
            if isinstance(external_access_integrations, str):
                external_access_integrations = [
                    x.strip()
                    for x in external_access_integrations.strip("[]").split(",")
                    if x.strip()
                ]
            elif not isinstance(external_access_integrations, list):
                external_access_integrations = None
        else:
            external_access_integrations = None

        return DBTObjectEditableAttributes(
            default_target=row_dict.get("default_target"),
            external_access_integrations=external_access_integrations,
            dbt_version=row_dict.get("dbt_version"),
        )

    def deploy(
        self,
        fqn: FQN,
        path: SecurePath,
        profiles_path: SecurePath,
        force: bool,
        attrs: DBTDeployAttributes,
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

        self._validate_profiles(profiles_path, profile, attrs.default_target)

        with cli_console.phase("Creating temporary stage"):
            stage_manager = StageManager()
            stage_fqn = FQN.from_resource(ObjectType.DBT_PROJECT, fqn, "STAGE")
            stage_manager.create(stage_fqn, temporary=True)
            stage_name = stage_manager.get_standard_stage_prefix(stage_fqn)

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
                return self._deploy_create_or_replace(fqn, stage_name, attrs)
            else:
                dbt_object_attributes = self.get_dbt_object_attributes(fqn)
                if dbt_object_attributes is not None:
                    return self._deploy_alter(
                        fqn, stage_name, dbt_object_attributes, attrs
                    )
                else:
                    return self._deploy_create(fqn, stage_name, attrs)

    def _deploy_alter(
        self,
        fqn: FQN,
        stage_name: str,
        dbt_object_attributes: DBTObjectEditableAttributes,
        attrs: DBTDeployAttributes,
    ) -> SnowflakeCursor:
        set_properties = []
        unset_properties = []

        current_default_target = dbt_object_attributes.get("default_target")
        if attrs.unset_default_target and current_default_target is not None:
            unset_properties.append("DEFAULT_TARGET")
        elif attrs.default_target and (
            current_default_target is None
            or current_default_target.lower() != attrs.default_target.lower()
        ):
            set_properties.append(f"DEFAULT_TARGET='{attrs.default_target}'")

        # Comparing dbt_version to existing project's dbt_version might be ambiguous
        # if previously project was locked to just minor version and now user wants to
        # lock it to a patch as well. If target version is provided, it's better to just
        # apply it.
        if attrs.dbt_version:
            set_properties.append(f"DBT_VERSION='{attrs.dbt_version}'")

        current_external_access_integrations = dbt_object_attributes.get(
            "external_access_integrations"
        )
        if self._should_update_external_access_integrations(
            current_external_access_integrations,
            attrs.external_access_integrations,
            attrs.install_local_deps,
        ):
            if attrs.external_access_integrations:
                integrations_str = ", ".join(sorted(attrs.external_access_integrations))
                set_properties.append(
                    f"EXTERNAL_ACCESS_INTEGRATIONS=({integrations_str})"
                )
            elif attrs.install_local_deps:
                set_properties.append("EXTERNAL_ACCESS_INTEGRATIONS=()")

        if set_properties or unset_properties:
            self._execute_property_updates(fqn, set_properties, unset_properties)

        query = f"ALTER DBT PROJECT {fqn} ADD VERSION"
        query += f"\nFROM {stage_name}"
        result = self.execute_query(query)

        return result

    @staticmethod
    def _should_update_external_access_integrations(
        current: Optional[List[str]],
        requested: Optional[List[str]],
        install_local_deps: bool,
    ) -> bool:
        if requested is not None:
            current_set = set(current) if current else set()
            requested_set = set(requested)
            return current_set != requested_set
        elif install_local_deps:
            current_set = set(current) if current else set()
            return current_set != set()
        return False

    def _execute_property_updates(
        self, fqn: FQN, set_clauses: List[str], unset_properties: List[str]
    ) -> None:
        if set_clauses:
            query = f"ALTER DBT PROJECT {fqn} SET {', '.join(set_clauses)}"
            self.execute_query(query)

        for property_name in unset_properties:
            query = f"ALTER DBT PROJECT {fqn} UNSET {property_name}"
            self.execute_query(query)

    def _deploy_create(
        self,
        fqn: FQN,
        stage_name: str,
        attrs: DBTDeployAttributes,
    ) -> SnowflakeCursor:
        query = f"CREATE DBT PROJECT {fqn}"
        query += f"\nFROM {stage_name}"
        if attrs.default_target:
            query += f" DEFAULT_TARGET='{attrs.default_target}'"
        if attrs.dbt_version:
            query += f" DBT_VERSION='{attrs.dbt_version}'"
        query = self._handle_external_access_integrations_query(
            query, attrs.external_access_integrations, attrs.install_local_deps
        )
        return self.execute_query(query)

    @staticmethod
    def _handle_external_access_integrations_query(
        query: str,
        external_access_integrations: Optional[List[str]],
        install_local_deps: bool,
    ) -> str:
        # Providing external access integrations will trigger installation of local deps as well
        if external_access_integrations:
            integrations_str = ", ".join(external_access_integrations)
            query += f"\nEXTERNAL_ACCESS_INTEGRATIONS = ({integrations_str})"
        elif install_local_deps:
            query += f"\nEXTERNAL_ACCESS_INTEGRATIONS = ()"
        return query

    def _deploy_create_or_replace(
        self,
        fqn: FQN,
        stage_name: str,
        attrs: DBTDeployAttributes,
    ) -> SnowflakeCursor:
        query = f"CREATE OR REPLACE DBT PROJECT {fqn}"
        query += f"\nFROM {stage_name}"
        if attrs.default_target:
            query += f" DEFAULT_TARGET='{attrs.default_target}'"
        if attrs.dbt_version:
            query += f" DBT_VERSION='{attrs.dbt_version}'"
        query = self._handle_external_access_integrations_query(
            query, attrs.external_access_integrations, attrs.install_local_deps
        )
        return self.execute_query(query)

    def _validate_profiles(
        self,
        profiles_path: SecurePath,
        profile_name: str,
        default_target: str | None = None,
    ) -> None:
        """
        Validates that:
         * profiles.yml exists
         * contain profile specified in dbt_project.yml
         * default_target (if specified) exists in the profile's outputs
        """
        profiles_file = profiles_path / PROFILES_FILENAME
        if not profiles_file.exists():
            raise CliError(
                f"{PROFILES_FILENAME} does not exist in directory {profiles_path.path.absolute()}."
            )
        with profiles_file.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
            profiles = yaml.safe_load(fd)

        if profile_name not in profiles:
            raise CliError(
                f"Profile {profile_name} is not defined in {PROFILES_FILENAME}."
            )

        errors = defaultdict(list)
        profile = profiles[profile_name]
        target_name = default_target or profile.get("target")
        available_targets = set(profile["outputs"].keys())
        if target_name in available_targets:
            target = profile["outputs"][target_name]
            target_errors = self._validate_target(target_name, target)
            if target_errors:
                errors[profile_name].extend(target_errors)
        else:
            available_targets_str = ", ".join(sorted(available_targets))
            errors[profile_name].append(
                f"Target '{target_name}' is not defined in profile '{profile_name}'. "
                f"Available targets: {available_targets_str}"
            )

        if errors:
            message = f"Found following errors in {PROFILES_FILENAME}. Please fix them before proceeding:"
            for target, issues in errors.items():
                message += f"\n{target}"
                message += "\n * " + "\n * ".join(issues)
            raise CliError(message)

    def _validate_target(
        self, target_name: str, target_details: Dict[str, str]
    ) -> List[str]:
        errors = []
        required_fields = {
            "database",
            "role",
            "schema",
            "type",
        }
        if missing_keys := required_fields - set(target_details.keys()):
            errors.append(
                f"Missing required fields: {', '.join(sorted(missing_keys))} in target {target_name}"
            )
        if role := target_details.get("role"):
            if not self._validate_role(role_name=role):
                errors.append(f"Role '{role}' does not exist or is not accessible.")
        return errors

    def _validate_role(self, role_name: str) -> bool:
        try:
            with self.use_role(role_name):
                self.execute_query("select 1")
            return True
        except ProgrammingError:
            return False

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
        self,
        dbt_command: str,
        name: FQN,
        run_async: bool,
        dbt_version: Optional[str] = None,
        *dbt_cli_args,
    ) -> SnowflakeCursor:
        if dbt_cli_args:
            processed_args = self._process_dbt_args(dbt_cli_args)
            dbt_command = f"{dbt_command} {processed_args}".strip()
        dbt_command_escaped = dbt_command.replace("'", "\\'")
        query = f"EXECUTE DBT PROJECT {name}"
        if dbt_version:
            query += f" dbt_version='{dbt_version}'"
        query += f" args='{dbt_command_escaped}'"
        return self.execute_query(query, _exec_async=run_async)

    @staticmethod
    def _process_dbt_args(dbt_cli_args: tuple) -> str:
        """
        Process dbt CLI arguments, handling special cases like --vars flag.
        """
        if not dbt_cli_args:
            return ""

        processed_args = []
        i = 0
        while i < len(dbt_cli_args):
            arg = dbt_cli_args[i]
            if arg == "--vars" and i + 1 < len(dbt_cli_args):
                vars_value = dbt_cli_args[i + 1]
                processed_args.append("--vars")
                processed_args.append(f"'{vars_value}'")
                i += 2
            else:
                processed_args.append(arg)
                i += 1
        return " ".join(processed_args)
