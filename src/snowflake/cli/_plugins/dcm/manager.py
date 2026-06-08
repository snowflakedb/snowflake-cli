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
from typing import TYPE_CHECKING, List, Optional

from snowflake.cli._plugins.dcm.models import MANIFEST_FILE_NAME, SOURCES_FOLDER
from snowflake.cli._plugins.dcm.utils import collect_output
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

if TYPE_CHECKING:
    from snowflake.cli._plugins.dcm.progress import DeployProgressTracker

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
    @property
    def connection(self):
        """Exposes the underlying Snowflake connection."""
        return self._conn

    def _build_deploy_query(
        self,
        project_identifier: FQN,
        from_stage: str,
        configuration: str | None,
        variables: List[str] | None,
        alias: str | None,
        skip_plan: bool,
    ) -> str:
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} DEPLOY"
        if alias:
            query += f' AS "{alias}"'
        query += self._get_configuration_and_variables_query(configuration, variables)
        query += self._get_from_stage_query(from_stage)
        if skip_plan:
            query += " SKIP PLAN"
        return query

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
        query = self._build_deploy_query(
            project_identifier, from_stage, configuration, variables, alias, skip_plan
        )
        return self.execute_query(query=query)

    def deploy_async(
        self,
        project_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        alias: str | None = None,
        skip_plan: bool = False,
    ) -> str:
        """
        Submits a deploy query asynchronously and returns the Snowflake query ID (sfqid).
        Use with :class:`~snowflake.cli._plugins.dcm.progress.DeployProgressTracker`
        to poll progress and obtain the final result cursor.
        """
        log.info(
            "Submitting DCM deploy async (project_identifier=%s, has_configuration=%s, variables_count=%d, skip_plan=%s).",
            project_identifier,
            bool(configuration),
            len(variables or []),
            skip_plan,
        )
        query = self._build_deploy_query(
            project_identifier, from_stage, configuration, variables, alias, skip_plan
        )
        # Closing the cursor does not cancel the async query; it keeps running
        # server-side and its results are fetched later via the sfqid (see
        # DeployProgressTracker.run_deploy_poll -> get_results_from_sfqid).
        with self._conn.cursor() as cursor:
            cursor.execute_async(query)
            sfqid = cursor.sfqid
        log.info(
            "DCM deploy async submitted (project_identifier=%s, sfqid=%s).",
            project_identifier,
            sfqid,
        )
        return sfqid

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
        delta: bool = False,
    ) -> SnowflakeCursor:
        log.info(
            "Running DCM plan manager operation (project_identifier=%s, has_configuration=%s, variables_count=%d, save_output=%s, delta=%s).",
            project_identifier,
            bool(configuration),
            len(variables or []),
            save_output,
            delta,
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} PLAN"
        if delta:
            query += " DELTA"
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
        query = self._build_purge_query(project_identifier, alias, skip_plan)
        return self.execute_query(query=query)

    def purge_async(
        self,
        project_identifier: FQN,
        alias: str | None = None,
        skip_plan: bool = False,
    ) -> str:
        """
        Submits a purge query asynchronously and returns the Snowflake query ID (sfqid).
        Use with :class:`~snowflake.cli._plugins.dcm.progress.DeployProgressTracker`
        to poll progress and obtain the final result cursor.
        """
        log.info(
            "Submitting DCM purge async (project_identifier=%s, skip_plan=%s).",
            project_identifier,
            skip_plan,
        )
        query = self._build_purge_query(project_identifier, alias, skip_plan)
        # Closing the cursor does not cancel the async query; results are
        # fetched later via the sfqid (see run_deploy_poll).
        with self._conn.cursor() as cursor:
            cursor.execute_async(query)
            sfqid = cursor.sfqid
        log.info(
            "DCM purge async submitted (project_identifier=%s, sfqid=%s).",
            project_identifier,
            sfqid,
        )
        return sfqid

    @staticmethod
    def _build_purge_query(
        project_identifier: FQN,
        alias: str | None,
        skip_plan: bool,
    ) -> str:
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} PURGE"
        if alias:
            query += f' AS "{alias}"'
        if skip_plan:
            query += " SKIP PLAN"
        return query

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
        project_identifier: FQN,
        source_directory: str | None = None,
        progress: Optional["DeployProgressTracker"] = None,
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
        return DCMProjectManager._sync_local_files_impl(
            project_identifier=project_identifier,
            source_path=source_path,
            progress=progress,
        )

    @staticmethod
    def _sync_local_files_impl(
        project_identifier: FQN,
        source_path: SecurePath,
        progress: Optional["DeployProgressTracker"],
    ) -> str:
        stage_fqn = FQN.from_resource(
            ObjectType.DCM_PROJECT, project_identifier, "TMP_STAGE"
        )
        plan = DCMProjectManager._build_upload_plan(
            source_path.path, stage_fqn.identifier
        )

        project_paths = ProjectPaths(project_root=source_path.path)
        project_paths.remove_up_bundle_root()
        SecurePath(project_paths.bundle_root).mkdir(parents=True, exist_ok=True)

        def _set_upload_details() -> None:
            parent_scope = project_identifier.prefix or str(project_identifier)
            stage_message = f"Creating temporary stage inside {parent_scope}."
            file_summaries = DCMProjectManager._summarize_upload_paths(
                plan.relative_paths_to_upload
            )
            if progress:
                progress.set_upload_context(
                    stage_message=stage_message,
                    file_summaries=file_summaries,
                )
            else:
                cli_console.step(stage_message)
                for summary in file_summaries:
                    cli_console.step(summary)

        try:
            bundle_artifacts(
                project_paths,
                plan.artifacts,
                pattern_type=PatternMatchingType.GLOB,
            )

            _set_upload_details()

            if progress:
                progress.set_upload_file_total(len(plan.relative_paths_to_upload))

            stage_manager = StageManager()
            stage_manager.create(
                fqn=FQN.from_stage(stage_fqn.identifier), temporary=True
            )

            for result in stage_manager.put_recursive(
                local_path=project_paths.bundle_root,
                stage_path=stage_fqn.identifier,
                temp_directory=project_paths.bundle_root,
            ):
                if progress:
                    progress.advance_upload()
                log.info(
                    "Uploaded %s to %s",
                    result["source"],
                    result["target"],
                )

            for entry in plan.individual_files:
                stage_manager.put(local_path=entry.file, stage_path=entry.dest)
                if progress:
                    progress.advance_upload()
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
        DCMProjectManager._add_manifest(plan)
        DCMProjectManager._add_sources(plan, source_path, stage_root)
        return plan

    @staticmethod
    def _add_manifest(plan: UploadPlan) -> None:
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
            dest_dir = f"{dest_dir}/{relative.parent.as_posix()}"
        return dest_dir

    @staticmethod
    def _summarize_upload_paths(relative_paths: List[str]) -> List[str]:
        """Summarize files to upload, grouped by sources/ subfolder."""
        if not relative_paths:
            return []

        manifest_count = 0
        folder_counts: dict[str, int] = {}

        for rel in relative_paths:
            if rel == MANIFEST_FILE_NAME:
                manifest_count += 1
            elif rel.startswith(f"{SOURCES_FOLDER}/"):
                parts = Path(rel).parts
                folder = (
                    "/".join(parts[:2]) + "/"
                    if len(parts) >= 3
                    else f"{SOURCES_FOLDER}/"
                )
                folder_counts[folder] = folder_counts.get(folder, 0) + 1
            else:
                folder_counts[rel] = folder_counts.get(rel, 0) + 1

        def _folder_label(folder: str) -> str:
            if folder == f"{SOURCES_FOLDER}/":
                return folder
            return folder.rstrip("/")

        lines: List[str] = []
        if manifest_count:
            lines.append(f"Upload {MANIFEST_FILE_NAME}")
        for folder in sorted(folder_counts):
            count = folder_counts[folder]
            # Pad the singular "file" with a trailing space so it aligns with
            # the plural "files" on adjacent rows in the upload details block.
            file_word = "file " if count == 1 else "files"
            lines.append(f"Upload {count} {file_word} from {_folder_label(folder)}")
        return lines
