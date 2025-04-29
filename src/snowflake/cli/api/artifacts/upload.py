from typing import List, Optional

from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.artifacts.utils import bundle_artifacts
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.entities.utils import sync_deploy_root_with_stage
from snowflake.cli.api.project.project_paths import ProjectPaths
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.secure_path import SecurePath


def sync_artifacts_with_stage(
    project_paths: ProjectPaths,
    stage_root: str,
    prune: bool = False,
    artifacts: Optional[List[PathMapping]] = None,
):
    if artifacts is None:
        artifacts = []

    project_paths.remove_up_bundle_root()
    SecurePath(project_paths.bundle_root).mkdir(parents=True, exist_ok=True)

    bundle_map = bundle_artifacts(project_paths, artifacts)
    stage_path_parts = StageManager().stage_path_parts_from_str(stage_root)
    # We treat the bundle root as deploy root
    sync_deploy_root_with_stage(
        console=cli_console,
        deploy_root=project_paths.bundle_root,
        bundle_map=bundle_map,
        prune=prune,
        recursive=True,
        stage_path=stage_path_parts,
        print_diff=True,
    )
    project_paths.clean_up_output()


def put_files(
    project_paths: ProjectPaths,
    stage_root: str,
    artifacts: Optional[List[PathMapping]] = None,
):
    if not artifacts:
        return

    sync_artifacts_with_stage(
        project_paths=project_paths,
        stage_root=stage_root,
        prune=False,
        artifacts=artifacts,
    )
