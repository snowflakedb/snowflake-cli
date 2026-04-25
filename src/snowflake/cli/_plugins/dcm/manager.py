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
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from snowflake.cli._plugins.dcm.models import MANIFEST_FILE_NAME, SOURCES_FOLDER
from snowflake.cli._plugins.dcm.utils import collect_output
from snowflake.cli._plugins.stage.diff import _to_diff_line
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.artifacts.utils import bundle_artifacts
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
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)


@dataclass
class FileUpload:
    file: Path
    dest: str


@dataclass
class UploadPlan:
    artifacts: List[PathMapping] = field(default_factory=list)
    individual_files: List[FileUpload] = field(default_factory=list)
    relative_paths_to_upload: List[str] = field(default_factory=list)


class DCMProjectManager(SqlExecutionMixin):
    def deploy(
        self,
        project_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        alias: str | None = None,
        skip_plan: bool = False,
    ) -> SnowflakeCursor:
        log.info(
            "Running DCM deploy manager operation (project_identifier=%s, has_configuration=%s, variables_count=%d, skip_plan=%s).",
            project_identifier,
            bool(configuration),
            len(variables or []),
            skip_plan,
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} DEPLOY"
        if alias:
            query += f' AS "{alias}"'
        query += self._get_configuration_and_variables_query(configuration, variables)
        query += self._get_from_stage_query(from_stage)
        if skip_plan:
            query += f" SKIP PLAN"
        return self.execute_query(query=query)

    def raw_analyze(
        self,
        project_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        save_output: bool = False,
    ):
        log.info(
            "Running DCM raw-analyze manager operation (project_identifier=%s, has_configuration=%s, variables_count=%d, save_output=%s).",
            project_identifier,
            bool(configuration),
            len(variables or []),
            save_output,
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} ANALYZE"
        query += self._get_configuration_and_variables_query(configuration, variables)
        query += self._get_from_stage_query(from_stage)

        if save_output:
            with collect_output(
                project_identifier, command_name="raw-analyze"
            ) as output_stage:
                query += f" OUTPUT_PATH {output_stage}"
                result = self.execute_query(query=query)
        else:
            result = self.execute_query(query=query)
        return result

    def plan(
        self,
        project_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        save_output: bool = False,
    ) -> SnowflakeCursor:
        log.info(
            "Running DCM plan manager operation (project_identifier=%s, has_configuration=%s, variables_count=%d, save_output=%s).",
            project_identifier,
            bool(configuration),
            len(variables or []),
            save_output,
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} PLAN"
        query += self._get_configuration_and_variables_query(configuration, variables)
        query += self._get_from_stage_query(from_stage)

        if save_output:
            with collect_output(
                project_identifier, command_name="plan"
            ) as output_stage:
                query += f" OUTPUT_PATH {output_stage}"
                result = self.execute_query(query=query)
        else:
            result = self.execute_query(query=query)
        return result

    def create(self, project_identifier: FQN) -> None:
        log.info(
            "Running DCM create manager operation (project_identifier=%s).",
            project_identifier,
        )
        query = f"CREATE DCM PROJECT {project_identifier.sql_identifier}"
        self.execute_query(query)

    def list_deployments(self, project_identifier: FQN) -> SnowflakeCursor:
        log.info(
            "Running DCM list-deployments manager operation (project_identifier=%s).",
            project_identifier,
        )
        query = f"SHOW DEPLOYMENTS IN DCM PROJECT {project_identifier.identifier}"
        return self.execute_query(query=query)

    def drop_deployment(
        self,
        project_identifier: FQN,
        deployment_name: str,
        if_exists: bool = False,
    ) -> None:
        """
        Drops a deployment from the DCM Project.
        """
        log.info(
            "Running DCM drop-deployment manager operation (project_identifier=%s, if_exists=%s).",
            project_identifier,
            if_exists,
        )
        query = f"ALTER DCM PROJECT {project_identifier.identifier} DROP DEPLOYMENT"
        if if_exists:
            query += " IF EXISTS"
        query += f' "{deployment_name}"'
        self.execute_query(query=query)

    def preview(
        self,
        project_identifier: FQN,
        object_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        limit: int | None = None,
    ) -> SnowflakeCursor:
        log.info(
            "Running DCM preview manager operation (project_identifier=%s, has_configuration=%s, variables_count=%d).",
            project_identifier,
            bool(configuration),
            len(variables or []),
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} PREVIEW {object_identifier.sql_identifier}"
        query += self._get_configuration_and_variables_query(configuration, variables)
        query += self._get_from_stage_query(from_stage)
        if limit is not None:
            query += f" LIMIT {limit}"
        return self.execute_query(query=query)

    def refresh(self, project_identifier: FQN) -> SnowflakeCursor:
        log.info(
            "Running DCM refresh manager operation (project_identifier=%s).",
            project_identifier,
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} REFRESH ALL"
        return self.execute_query(query=query)

    def purge(
        self,
        project_identifier: FQN,
        alias: str | None = None,
        skip_plan: bool = False,
    ) -> SnowflakeCursor:
        log.info(
            "Running DCM purge manager operation (project_identifier=%s, skip_plan=%s).",
            project_identifier,
            skip_plan,
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} PURGE"
        if alias:
            query += f' AS "{alias}"'
        if skip_plan:
            query += " SKIP PLAN"
        return self.execute_query(query=query)

    def test(self, project_identifier: FQN) -> SnowflakeCursor:
        log.info(
            "Running DCM test manager operation (project_identifier=%s).",
            project_identifier,
        )
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
        log.info(
            "Syncing local DCM files to temporary stage (project_identifier=%s, source_directory=%s).",
            project_identifier,
            source_path,
        )

        with cli_console.phase("Uploading definition files"):
            stage_fqn = FQN.from_resource(
                ObjectType.DCM_PROJECT, project_identifier, "TMP_STAGE"
            )
            plan = DCMProjectManager._build_upload_plan(
                source_path.path, stage_fqn.identifier
            )

            project_paths = ProjectPaths(project_root=source_path.path)
            project_paths.remove_up_bundle_root()
            SecurePath(project_paths.bundle_root).mkdir(parents=True, exist_ok=True)

            try:
                bundle_artifacts(
                    project_paths,
                    plan.artifacts,
                    pattern_type=PatternMatchingType.GLOB,
                )

                stage_manager = StageManager()
                cli_console.step(f"Creating temporary stage {stage_fqn.identifier}.")
                stage_manager.create(
                    fqn=FQN.from_stage(stage_fqn.identifier), temporary=True
                )

                DCMProjectManager._report_files_to_be_deployed(plan)

                cli_console.step(
                    f"Uploading files from local {project_paths.bundle_root} directory to temporary stage."
                )
                for result in stage_manager.put_recursive(
                    local_path=project_paths.bundle_root,
                    stage_path=stage_fqn.identifier,
                    temp_directory=project_paths.bundle_root,
                ):
                    log.info(
                        "Uploaded %s to %s",
                        result["source"],
                        result["target"],
                    )

                for entry in plan.individual_files:
                    stage_manager.put(local_path=entry.file, stage_path=entry.dest)
                    log.info(
                        "Uploaded %s to %s",
                        entry.file.relative_to(source_path.path),
                        entry.dest,
                    )
            finally:
                project_paths.clean_up_output()

        log.info(
            "Finished syncing DCM files (project_identifier=%s, stage=%s).",
            project_identifier,
            stage_fqn.identifier,
        )
        return stage_fqn.identifier

    @staticmethod
    def _build_upload_plan(source_path: Path, stage_root: str) -> UploadPlan:
        plan = UploadPlan()
        DCMProjectManager._add_manifest(plan, source_path)
        DCMProjectManager._add_sources(plan, source_path, stage_root)
        return plan

    @staticmethod
    def _add_manifest(plan: UploadPlan, source_path: Path) -> None:
        if (source_path / MANIFEST_FILE_NAME).exists():
            plan.artifacts.append(PathMapping(src=MANIFEST_FILE_NAME))
            plan.relative_paths_to_upload.append(MANIFEST_FILE_NAME)

    @staticmethod
    def _add_sources(plan: UploadPlan, source_path: Path, stage_root: str) -> None:
        sources_path = source_path / SOURCES_FOLDER
        if not (sources_path.exists() and sources_path.is_dir()):
            return
        plan.artifacts.append(PathMapping(src=SOURCES_FOLDER, ignore=[".*"]))
        for file in sorted(sources_path.rglob("*")):
            if not file.is_file():
                continue
            relative = file.relative_to(sources_path)
            plan.relative_paths_to_upload.append(f"{SOURCES_FOLDER}/{relative}")
            if DCMProjectManager._is_in_hidden_path(relative):
                dest_dir = DCMProjectManager._sources_stage_destination(
                    relative, stage_root
                )
                plan.individual_files.append(FileUpload(file=file, dest=dest_dir))

    @staticmethod
    def _is_in_hidden_path(relative: Path) -> bool:
        return any(part.startswith(".") for part in relative.parts)

    @staticmethod
    def _sources_stage_destination(relative: Path, stage_root: str) -> str:
        dest_dir = f"{stage_root}/{SOURCES_FOLDER}"
        if relative.parent != Path("."):
            dest_dir = f"{dest_dir}/{relative.parent}"
        return dest_dir

    @staticmethod
    def _report_files_to_be_deployed(plan: UploadPlan) -> None:
        if not plan.relative_paths_to_upload:
            return

        cli_console.message("Local changes to be deployed:")
        with cli_console.indented():
            for rel in plan.relative_paths_to_upload:
                cli_console.message(_to_diff_line("added", rel, rel))
