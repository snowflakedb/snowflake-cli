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
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List

from snowflake.cli._plugins.dcm.models import MANIFEST_FILE_NAME
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.artifacts.upload import sync_artifacts_with_stage
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import (
    ObjectType,
    PatternMatchingType,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.project_paths import ProjectPaths
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.stage_path import StagePath

SOURCES_FOLDER = "sources"
OUTPUT_FOLDER = "out"


class DCMProjectManager(SqlExecutionMixin):
    @contextmanager
    def _collect_output(self, project_identifier: FQN) -> Generator[str, None, None]:
        """
        Context manager for handling plan output - creates temporary stage,
        downloads files to out/ folder after execution.

        Args:
            project_identifier: The DCM project identifier

        Yields:
            str: The effective output path to use in the DCM command
        """
        stage_manager = StageManager()
        temp_stage_fqn = FQN.from_resource(
            ObjectType.DCM_PROJECT, project_identifier, "OUTPUT_TMP_STAGE"
        )
        stage_manager.create(temp_stage_fqn, temporary=True)
        effective_output_path = StagePath.from_stage_str(
            temp_stage_fqn.identifier
        ).joinpath("/outputs")
        local_output_path = SecurePath(OUTPUT_FOLDER)

        try:
            yield effective_output_path.absolute_path()
        finally:
            stage_manager.get_recursive(
                stage_path=effective_output_path.absolute_path(),
                dest_path=local_output_path.path,
            )
            cli_console.step(f"Plan output saved to: {local_output_path.resolve()}")

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
        save_output: bool = False,
    ):
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} PLAN"
        query += self._get_configuration_and_variables_query(configuration, variables)
        query += self._get_from_stage_query(from_stage)

        if save_output:
            with self._collect_output(project_identifier) as output_stage:
                query += f" OUTPUT_PATH {output_stage}"
                result = self.execute_query(query=query)
        else:
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
    def sync_local_files(
        project_identifier: FQN, source_directory: str | None = None
    ) -> str:
        source_path = (
            SecurePath(source_directory).resolve()
            if source_directory
            else SecurePath.cwd()
        )

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
        """Collect all artifacts from sources/ folder and manifest.yml."""
        artifacts: List[PathMapping] = []

        artifacts.append(PathMapping(src=MANIFEST_FILE_NAME))

        sources_path = source_path / SOURCES_FOLDER
        if sources_path.exists() and sources_path.is_dir():
            for file in sources_path.rglob("*"):
                if file.is_file():
                    relative_path = file.relative_to(source_path)
                    artifacts.append(PathMapping(src=str(relative_path)))

        return artifacts
