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
import time
from pathlib import Path
from typing import List

import yaml
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.artifacts.upload import sync_artifacts_with_stage
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB, PatternMatchingType
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.project_paths import ProjectPaths
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.project.util import unquote_identifier
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.stage_path import StagePath

MANIFEST_FILE_NAME = "manifest.yml"
DCM_PROJECT_TYPE = "dcm_project"


class DCMProjectManager(SqlExecutionMixin):
    def execute(
        self,
        project_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        dry_run: bool = False,
        alias: str | None = None,
        output_path: str | None = None,
    ):

        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier}"
        if dry_run:
            query += " PLAN"
        else:
            query += " DEPLOY"
            if alias:
                query += f' AS "{alias}"'
        if configuration or variables:
            query += f" USING"
        if configuration:
            query += f" CONFIGURATION {configuration}"
        if variables:
            query += StageManager.parse_execute_variables(
                parse_key_value_variables(variables)
            ).removeprefix(" using")
        stage_path = StagePath.from_stage_str(from_stage)
        query += f" FROM {stage_path.absolute_path()}"
        if output_path:
            output_stage_path = StagePath.from_stage_str(output_path)
            query += f" OUTPUT_PATH {output_stage_path.absolute_path()}"
        return self.execute_query(query=query)

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

    @staticmethod
    def sync_local_files(project_identifier: FQN) -> str:
        dcm_manifest_file = SecurePath.cwd() / MANIFEST_FILE_NAME
        if not dcm_manifest_file.exists():
            raise CliError(f"{MANIFEST_FILE_NAME} was not found in project directory")

        with dcm_manifest_file.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
            dcm_manifest = yaml.safe_load(fd)
            object_type = dcm_manifest.get("type") if dcm_manifest else None
            if object_type is None:
                raise CliError(
                    f"Manifest file type is undefined. Expected {DCM_PROJECT_TYPE}"
                )
            if object_type.lower() != DCM_PROJECT_TYPE:
                raise CliError(
                    f"Manifest file is defined for type {object_type}. Expected {DCM_PROJECT_TYPE}"
                )

            definitions = list(dcm_manifest.get("include_definitions", list()))
            if MANIFEST_FILE_NAME not in definitions:
                definitions.append(MANIFEST_FILE_NAME)

        with cli_console.phase(f"Uploading definition files"):
            unquoted_name = unquote_identifier(project_identifier.name)
            stage_fqn = FQN.from_string(
                f"DCM_{unquoted_name}_{int(time.time())}_TMP_STAGE"
            ).using_context()
            sync_artifacts_with_stage(
                project_paths=ProjectPaths(project_root=Path.cwd()),
                stage_root=stage_fqn.identifier,
                use_temporary_stage=True,
                artifacts=[PathMapping(src=definition) for definition in definitions],
                pattern_type=PatternMatchingType.REGEX,
            )

        return stage_fqn.identifier
