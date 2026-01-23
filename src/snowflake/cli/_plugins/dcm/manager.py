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
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import yaml
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.artifacts.upload import sync_artifacts_with_stage
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import (
    DEFAULT_SIZE_LIMIT_MB,
    ObjectType,
    PatternMatchingType,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.project_paths import ProjectPaths
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.stage_path import StagePath
from snowflake.cli.api.utils.path_utils import is_stage_path

MANIFEST_FILE_NAME = "manifest.yml"
DCM_PROJECT_TYPE = "dcm_project"
DEFINITIONS_FOLDER = "definitions"
MACROS_FOLDER = "macros"
REQUIRED_MANIFEST_VERSION = "2.0"


@dataclass
class DCMTemplating:
    """Templating configuration for DCM manifest v2."""

    global_variables: Dict[str, Any] = field(default_factory=dict)
    configurations: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "DCMTemplating":
        if not data:
            return cls()
        return cls(
            global_variables=data.get("global_variables", {}),
            configurations=data.get("configurations", {}),
        )


@dataclass
class DCMManifest:
    """DCM manifest v2 structure."""

    manifest_version: str
    project_type: str
    templating: DCMTemplating = field(default_factory=DCMTemplating)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DCMManifest":
        return cls(
            manifest_version=str(data.get("manifest_version", "")),
            project_type=data.get("type", ""),
            templating=DCMTemplating.from_dict(data.get("templating")),
        )

    def validate(self) -> None:
        """Validate the manifest structure."""
        if not self.project_type:
            raise CliError(
                f"Manifest file type is undefined. Expected {DCM_PROJECT_TYPE}."
            )
        if self.project_type.lower() != DCM_PROJECT_TYPE:
            raise CliError(
                f"Manifest file is defined for type {self.project_type}. Expected {DCM_PROJECT_TYPE}."
            )
        if self.manifest_version != REQUIRED_MANIFEST_VERSION:
            raise CliError(
                f"Manifest version '{self.manifest_version}' is not supported. Expected {REQUIRED_MANIFEST_VERSION}."
            )

    def get_configuration_names(self) -> List[str]:
        """Return list of available configuration names."""
        return list(self.templating.configurations.keys())


class DCMProjectManager(SqlExecutionMixin):
    @contextmanager
    def _collect_output(
        self, project_identifier: FQN, output_path: str
    ) -> Generator[str, None, None]:
        """
        Context manager for handling output path - creates temporary stage for local paths,
        downloads files after execution, and ensures proper cleanup.

        Args:
            project_identifier: The DCM project identifier
            output_path: Either a stage path (@stage/path) or local directory path

        Yields:
            str: The effective output path to use in the DCM command
        """
        temp_stage_for_local_output = None
        stage_manager = StageManager()

        if should_download_files := not is_stage_path(output_path):
            temp_stage_fqn = FQN.from_resource(
                ObjectType.DCM_PROJECT, project_identifier, "OUTPUT_TMP_STAGE"
            )
            stage_manager.create(temp_stage_fqn, temporary=True)
            effective_output_path = StagePath.from_stage_str(
                temp_stage_fqn.identifier
            ).joinpath("/outputs")
            temp_stage_for_local_output = (temp_stage_fqn.identifier, Path(output_path))
        else:
            effective_output_path = StagePath.from_stage_str(output_path)

        try:
            yield effective_output_path.absolute_path()
        finally:
            if should_download_files:
                assert temp_stage_for_local_output is not None
                stage_path, local_path = temp_stage_for_local_output
                stage_manager.get_recursive(
                    stage_path=effective_output_path.absolute_path(),
                    dest_path=local_path,
                )
                cli_console.step(f"Plan output saved to: {local_path.resolve()}")
            else:
                cli_console.step(f"Plan output saved to: {output_path}")

    def deploy(
        self,
        project_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        alias: str | None = None,
        skip_plan: bool = False,
    ):
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} DEPLOY"
        if alias:
            query += f' AS "{alias}"'
        query += self._get_configuration_and_variables_query(configuration, variables)
        query += self._get_from_stage_query(from_stage)
        if skip_plan:
            query += f" SKIP PLAN"
        return self.execute_query(query=query)

    def plan(
        self,
        project_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        output_path: str | None = None,
    ):
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} PLAN"
        query += self._get_configuration_and_variables_query(configuration, variables)
        query += self._get_from_stage_query(from_stage)
        with self._collect_output(
            project_identifier, output_path
        ) if output_path else nullcontext() as output_stage:
            if output_stage is not None:
                query += f" OUTPUT_PATH {output_stage}"
            result = self.execute_query(query=query)

        return result

    def create(self, project_identifier: FQN) -> None:
        query = f"CREATE DCM PROJECT {project_identifier.sql_identifier}"
        self.execute_query(query)

    def list_deployments(self, project_identifier: FQN):
        query = f"SHOW DEPLOYMENTS IN DCM PROJECT {project_identifier.identifier}"
        return self.execute_query(query=query)

    def drop_deployment(
        self,
        project_identifier: FQN,
        deployment_name: str,
        if_exists: bool = False,
    ):
        """
        Drops a deployment from the DCM Project.
        """
        query = f"ALTER DCM PROJECT {project_identifier.identifier} DROP DEPLOYMENT"
        if if_exists:
            query += " IF EXISTS"
        query += f' "{deployment_name}"'
        return self.execute_query(query=query)

    def preview(
        self,
        project_identifier: FQN,
        object_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        limit: int | None = None,
    ):
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} PREVIEW {object_identifier.sql_identifier}"
        query += self._get_configuration_and_variables_query(configuration, variables)
        query += self._get_from_stage_query(from_stage)
        if limit is not None:
            query += f" LIMIT {limit}"
        return self.execute_query(query=query)

    def refresh(self, project_identifier: FQN):
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} REFRESH ALL"
        return self.execute_query(query=query)

    def test(self, project_identifier: FQN):
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} TEST ALL"
        return self.execute_query(query=query)

    @staticmethod
    def _get_from_stage_query(from_stage: str) -> str:
        stage_path = StagePath.from_stage_str(from_stage)
        return f" FROM {stage_path.absolute_path()}"

    @staticmethod
    def _get_configuration_and_variables_query(
        configuration: str | None, variables: List[str] | None
    ) -> str:
        query = ""
        if configuration or variables:
            query += f" USING"
        if configuration:
            query += f" CONFIGURATION {configuration}"
        if variables:
            query += StageManager.parse_execute_variables(
                parse_key_value_variables(variables)
            ).removeprefix(" using")
        return query

    @staticmethod
    def load_manifest(source_path: SecurePath) -> DCMManifest:
        """Load and validate manifest from the given path."""
        dcm_manifest_file = source_path / MANIFEST_FILE_NAME
        if not dcm_manifest_file.exists():
            raise CliError(
                f"{MANIFEST_FILE_NAME} was not found in directory {source_path.path}."
            )

        with dcm_manifest_file.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
            data = yaml.safe_load(fd)
            if not data:
                raise CliError("Manifest file is empty or invalid.")

            manifest = DCMManifest.from_dict(data)
            manifest.validate()
            return manifest

    @staticmethod
    def sync_local_files(
        project_identifier: FQN, source_directory: str | None = None
    ) -> str:
        source_path = (
            SecurePath(source_directory).resolve()
            if source_directory
            else SecurePath.cwd()
        )

        DCMProjectManager.load_manifest(source_path)

        artifacts = DCMProjectManager._collect_artifacts(source_path.path)

        with cli_console.phase("Uploading definition files"):
            stage_fqn = FQN.from_resource(
                ObjectType.DCM_PROJECT, project_identifier, "TMP_STAGE"
            )
            sync_artifacts_with_stage(
                project_paths=ProjectPaths(project_root=source_path.path),
                stage_root=stage_fqn.identifier,
                use_temporary_stage=True,
                artifacts=artifacts,
                pattern_type=PatternMatchingType.GLOB,
            )

        return stage_fqn.identifier

    @staticmethod
    def _collect_artifacts(source_path: Path) -> List[PathMapping]:
        """Collect all artifacts from definitions/, macros/ folders and manifest.yml."""
        artifacts: List[PathMapping] = []

        # Add manifest file
        artifacts.append(PathMapping(src=MANIFEST_FILE_NAME))

        # Add all .sql files from definitions/ folder recursively
        definitions_path = source_path / DEFINITIONS_FOLDER
        if definitions_path.exists() and definitions_path.is_dir():
            for sql_file in definitions_path.rglob("*.sql"):
                relative_path = sql_file.relative_to(source_path)
                artifacts.append(PathMapping(src=str(relative_path)))

        # Add all files from macros/ folder recursively
        macros_path = source_path / MACROS_FOLDER
        if macros_path.exists() and macros_path.is_dir():
            for macro_file in macros_path.rglob("*"):
                if macro_file.is_file():
                    relative_path = macro_file.relative_to(source_path)
                    artifacts.append(PathMapping(src=str(relative_path)))

        return artifacts
