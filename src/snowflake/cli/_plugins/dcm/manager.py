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
import glob
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path, PurePath
from tempfile import TemporaryDirectory
from typing import List, Set

from snowflake.cli._plugins.dcm.models import (
    MANIFEST_FILE_NAME,
    SOURCES_FOLDER,
    DCMAsset,
)
from snowflake.cli._plugins.dcm.multistep_progress import StepProgressUpdater
from snowflake.cli._plugins.dcm.progress import FileUploadProgress, upload_details
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.artifacts.utils import symlink_or_copy
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector import SnowflakeConnection
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
    relative_paths_to_upload: List[PurePath] = field(default_factory=list)


def _escape_literal_brackets(pattern: str) -> str:
    """Make ``[``/``]`` literal for the glob engine.

    Per the assets spec ``[`` and ``]`` match literally, but ``pathlib.Path.glob``
    treats ``[...]`` as a character class. Escaping ``[`` to the class ``[[]``
    keeps brackets literal while leaving ``*``/``**`` as wildcards (a lone ``]``
    is already literal in glob). ``str.replace`` is safe because ``[`` is never
    part of ``*``/``**``.
    """
    return pattern.replace("[", "[[]")


def _is_hidden(relative: Path) -> bool:
    """True if any path component is dot-prefixed (a dotfile or dot-directory)."""
    return any(part.startswith(".") for part in relative.parts)


def _stays_in_project_root(path: Path, real_root: Path) -> bool:
    """True if ``path``'s real (symlink-resolved) location is inside ``real_root``.

    ``Path.glob``/``rglob`` follow symlinks, so a symlink inside the project that
    points outside it (e.g. ``link -> /etc``) would otherwise be bundled. Compare
    the fully-resolved real paths to block that escape.
    """
    try:
        real = path.resolve()
    except OSError as exc:
        log.debug(
            "Skipping asset path %s: could not resolve its real location (%s).",
            path,
            exc,
        )
        return False
    return real == real_root or real_root in real.parents


def _resolve_asset_pattern(project_root: Path, pattern: str) -> List[Path]:
    """Resolve one ``path``/``paths`` entry to repo-relative files, per spec.

    * a literal file → itself
    * a literal directory → its whole subtree
    * a glob (contains ``*``) → matching files only; a matched *directory* is
      not descended into (``*`` / ``apps/*`` select one level, ``**/*`` recurses)

    Patterns are pre-validated (DEX-47) to use only ``*``/``**``, so ``Path.glob``
    is a safe superset once literal ``[``/``]`` are escaped. Matches are always
    ``/``-separated on input and emitted via ``as_posix()`` so stage paths are
    correct on every OS. Dotfiles are excluded -- ``put_recursive`` can't upload
    them (tracked in DEX-51) -- so a pattern matching only dotfiles counts as no
    match. Symlinks that resolve outside the project root are skipped.
    """
    escaped = _escape_literal_brackets(pattern)
    is_glob = "*" in pattern
    real_root = project_root.resolve()
    files: List[Path] = []
    for match in sorted(project_root.glob(escaped)):
        relative = match.relative_to(project_root)
        if _is_hidden(relative) or not _stays_in_project_root(match, real_root):
            log.debug(
                "Skipping asset match %s (dotfile or outside project root).", relative
            )
            continue
        if match.is_file():
            files.append(relative)
        elif match.is_dir() and not is_glob:
            for child in sorted(match.rglob("*")):
                child_relative = child.relative_to(project_root)
                if (
                    child.is_file()
                    and not _is_hidden(child_relative)
                    and _stays_in_project_root(child, real_root)
                ):
                    files.append(child_relative)
    return files


def resolve_asset_paths(project_root: Path, assets: List[DCMAsset]) -> List[str]:
    """Resolve every asset pattern to a sorted, de-duplicated list of
    repo-relative POSIX file paths. A pattern that matches no file is skipped
    with a warning rather than failing the whole command.
    """
    resolved: Set[str] = set()
    for asset in assets:
        for pattern in asset.paths:
            matches = _resolve_asset_pattern(project_root, pattern)
            if not matches:
                cli_console.warning(
                    f"Asset '{asset.name}' path '{pattern}' matched no files; skipping."
                )
                continue
            resolved.update(match.as_posix() for match in matches)
    return sorted(resolved)


class DCMProjectManager(SqlExecutionMixin):
    @property
    def connection(self) -> SnowflakeConnection:
        return self._conn

    def deploy_async(
        self,
        project_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        alias: str | None = None,
        skip_plan: bool = False,
        env_vars: dict[str, str] | None = None,
    ) -> str:
        log.info(
            "Submitting DCM deploy async (project_identifier=%s, has_configuration=%s, variables_count=%d, skip_plan=%s).",
            project_identifier,
            bool(configuration),
            len(variables or []),
            skip_plan,
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} DEPLOY"
        if alias:
            query += f' AS "{alias}"'
        query += self._get_configuration_and_variables_query(configuration, variables)
        if env_vars:
            query += " ENVIRONMENT (?)"
        query += self._get_from_stage_query(from_stage)
        if skip_plan:
            query += f" SKIP PLAN"
        cursor = self._execute_with_optional_env_vars_async(query, env_vars)
        log.info(
            "DCM deploy async submitted (project_identifier=%s, sfqid=%s).",
            project_identifier,
            cursor.sfqid,
        )
        return cursor.sfqid

    def raw_analyze(
        self,
        project_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        output_path: str | None = None,
        env_vars: dict[str, str] | None = None,
    ):
        log.info(
            "Running DCM raw-analyze manager operation (project_identifier=%s, has_configuration=%s, variables_count=%d, has_output_path=%s).",
            project_identifier,
            bool(configuration),
            len(variables or []),
            bool(output_path),
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} ANALYZE"
        query += self._get_configuration_and_variables_query(configuration, variables)
        if env_vars:
            query += " ENVIRONMENT (?)"
        query += self._get_from_stage_query(from_stage)
        if output_path:
            query += f" OUTPUT_PATH {output_path}"
        return self._execute_with_optional_env_vars(query, env_vars)

    def plan_async(
        self,
        project_identifier: FQN,
        from_stage: str,
        configuration: str | None = None,
        variables: List[str] | None = None,
        delta: bool = False,
        output_path: str | None = None,
        env_vars: dict[str, str] | None = None,
    ) -> str:
        log.info(
            "Submitting DCM plan async (project_identifier=%s, has_configuration=%s, variables_count=%d, has_output_path=%s, delta=%s).",
            project_identifier,
            bool(configuration),
            len(variables or []),
            bool(output_path),
            delta,
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} PLAN"
        if delta:
            query += " DELTA"
        query += self._get_configuration_and_variables_query(configuration, variables)
        if env_vars:
            query += " ENVIRONMENT (?)"
        query += self._get_from_stage_query(from_stage)
        if output_path:
            query += f" OUTPUT_PATH {output_path}"
        cursor = self._execute_with_optional_env_vars_async(query, env_vars)
        log.info(
            "DCM plan async submitted (project_identifier=%s, sfqid=%s).",
            project_identifier,
            cursor.sfqid,
        )
        return cursor.sfqid

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
        query = f"SHOW DEPLOYMENTS IN DCM PROJECT {project_identifier.sql_identifier}"
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
        query = f"ALTER DCM PROJECT {project_identifier.sql_identifier} DROP DEPLOYMENT"
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
        env_vars: dict[str, str] | None = None,
    ) -> SnowflakeCursor:
        log.info(
            "Running DCM preview manager operation (project_identifier=%s, has_configuration=%s, variables_count=%d).",
            project_identifier,
            bool(configuration),
            len(variables or []),
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} PREVIEW {object_identifier.sql_identifier}"
        query += self._get_configuration_and_variables_query(configuration, variables)
        if env_vars:
            query += " ENVIRONMENT (?)"
        query += self._get_from_stage_query(from_stage)
        if limit is not None:
            query += f" LIMIT {limit}"
        return self._execute_with_optional_env_vars(query, env_vars)

    def refresh(self, project_identifier: FQN) -> SnowflakeCursor:
        log.info(
            "Running DCM refresh manager operation (project_identifier=%s).",
            project_identifier,
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} REFRESH ALL"
        return self.execute_query(query=query)

    def purge_async(
        self,
        project_identifier: FQN,
        alias: str | None = None,
        skip_plan: bool = False,
    ) -> str:
        log.info(
            "Submitting DCM purge async (project_identifier=%s, skip_plan=%s).",
            project_identifier,
            skip_plan,
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} PURGE"
        if alias:
            query += f' AS "{alias}"'
        if skip_plan:
            query += " SKIP PLAN"
        cursor = self.execute_query_with_params_async(query=query)
        log.info(
            "DCM purge async submitted (project_identifier=%s, sfqid=%s).",
            project_identifier,
            cursor.sfqid,
        )
        return cursor.sfqid

    def test(self, project_identifier: FQN) -> SnowflakeCursor:
        log.info(
            "Running DCM test manager operation (project_identifier=%s).",
            project_identifier,
        )
        query = f"EXECUTE DCM PROJECT {project_identifier.sql_identifier} TEST ALL"
        return self.execute_query(query=query)

    def _execute_with_optional_env_vars(
        self, query: str, env_vars: dict[str, str] | None
    ) -> SnowflakeCursor:
        if env_vars:
            return self.execute_query_with_params(
                query=query, params=[json.dumps(env_vars)]
            )
        return self.execute_query(query=query)

    def _execute_with_optional_env_vars_async(
        self, query: str, env_vars: dict[str, str] | None
    ) -> SnowflakeCursor:
        params = [json.dumps(env_vars)] if env_vars else None
        return self.execute_query_with_params_async(query=query, params=params)

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
        progress: StepProgressUpdater,
        source_directory: str | None = None,
        assets: List[DCMAsset] | None = None,
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
        stage_fqn = FQN.from_resource(
            ObjectType.DCM_PROJECT, project_identifier, "TMP_STAGE"
        )
        plan = DCMProjectManager._build_upload_plan(
            source_path.path, stage_fqn.identifier, assets=assets or []
        )

        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            DCMProjectManager._bundle_definition_files(
                project_root=source_path.path,
                bundle_root=tmp_path,
                artifacts=plan.artifacts,
            )

            stage_manager = StageManager()
            uploader = FileUploadProgress(progress, len(plan.relative_paths_to_upload))
            progress.set_details(
                upload_details(stage_fqn, plan.relative_paths_to_upload)
            )

            stage_manager.create(
                fqn=FQN.from_stage(stage_fqn.identifier), temporary=True
            )
            for result in stage_manager.put_recursive(
                local_path=tmp_path,
                stage_path=stage_fqn.identifier,
                temp_directory=tmp_path,
                # DCM uploads many small files; spend the whole upload
                # concurrency budget on directory fan-out rather than
                # per-PUT PARALLEL (parallel=1 => fan-out == full budget).
                parallel=1,
            ):
                uploader.advance()
                log.info(
                    "Uploaded %s to %s",
                    result["source"],
                    result["target"],
                )
            for entry in plan.individual_files:
                stage_manager.put(local_path=entry.file, stage_path=entry.dest)
                uploader.advance()
                log.info(
                    "Uploaded %s to %s",
                    entry.file.relative_to(source_path.path),
                    entry.dest,
                )

        log.info(
            "Finished syncing DCM files (project_identifier=%s, stage=%s).",
            project_identifier,
            stage_fqn.identifier,
        )
        return stage_fqn.identifier

    @staticmethod
    def _bundle_definition_files(
        project_root: Path, bundle_root: Path, artifacts: List[PathMapping]
    ) -> None:
        bundle_map = BundleMap(
            project_root=project_root,
            deploy_root=bundle_root,
        )
        for artifact in artifacts:
            bundle_map.add(artifact)

        for absolute_src, absolute_dest in bundle_map.all_mappings(
            absolute=True, expand_directories=True
        ):
            if absolute_src.is_file():
                symlink_or_copy(
                    absolute_src,
                    absolute_dest,
                    deploy_root=bundle_root,
                    project_root=project_root,
                )

    @staticmethod
    def _build_upload_plan(
        source_path: Path,
        stage_root: str,
        assets: List[DCMAsset],
    ) -> UploadPlan:
        plan = UploadPlan()
        DCMProjectManager._add_manifest(plan)
        DCMProjectManager._add_sources(plan, source_path, stage_root)
        DCMProjectManager._add_assets(plan, source_path, assets)
        return plan

    @staticmethod
    def _add_manifest(plan: UploadPlan) -> None:
        plan.artifacts.append(PathMapping(src=MANIFEST_FILE_NAME))
        plan.relative_paths_to_upload.append(PurePath(MANIFEST_FILE_NAME))

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
            plan.relative_paths_to_upload.append(
                DCMProjectManager._sources_relative_path(relative)
            )
            if DCMProjectManager._is_in_hidden_path(relative):
                dest_dir = DCMProjectManager._sources_stage_destination(
                    relative, stage_root
                )
                plan.individual_files.append(FileUpload(file=file, dest=dest_dir))

    @staticmethod
    def _add_assets(
        plan: UploadPlan, source_path: Path, assets: List[DCMAsset]
    ) -> None:
        if not assets:
            return

        # Resolve declared patterns to concrete files (spec semantics), then
        # bundle each as its own artifact. Feeding concrete file paths means
        # BundleMap copies exactly these files with no directory re-expansion.
        # `seen` is seeded from manifest + sources/, so an asset that overlaps
        # files already shipped by the default DCM path is a no-op here.
        seen = set(plan.relative_paths_to_upload)
        for relative_posix in resolve_asset_paths(source_path, assets):
            relative_path = PurePath(relative_posix)
            if relative_path in seen:
                continue
            seen.add(relative_path)
            # `relative_posix` is a concrete resolved file, but BundleMap re-globs
            # every PathMapping.src -- so escape *all* wildcard chars ('*', '?', '['),
            # not just '['. Otherwise a real filename like 'backup*' re-expands on the
            # second pass and uploads an unrelated directory subtree. glob.escape emits
            # [*]/[?]/[[], composing with GLOB matching.
            plan.artifacts.append(PathMapping(src=glob.escape(relative_posix)))
            plan.relative_paths_to_upload.append(relative_path)

    @staticmethod
    def _is_in_hidden_path(relative: Path) -> bool:
        return _is_hidden(relative)

    @staticmethod
    def _sources_relative_path(relative: PurePath) -> PurePath:
        return PurePath(SOURCES_FOLDER, *relative.parts)

    @staticmethod
    def _sources_stage_destination(relative: Path, stage_root: str) -> str:
        dest_dir = f"{stage_root}/{SOURCES_FOLDER}"
        if relative.parent != Path("."):
            dest_dir = f"{dest_dir}/{relative.parent.as_posix()}"
        return dest_dir
