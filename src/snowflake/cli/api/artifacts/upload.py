from pathlib import PurePosixPath
from typing import List, Optional

from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.artifacts.utils import symlink_or_copy
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.project.project_paths import ProjectPaths
from snowflake.cli.api.project.schemas.entities.common import PathMapping


def put_files(
    project_paths: ProjectPaths,
    stage_root: str,
    artifacts: Optional[List[PathMapping]] = None,
):
    if not artifacts:
        return
    stage_manager = StageManager()
    # We treat the bundle root as deploy root
    bundle_map = BundleMap(
        project_root=project_paths.project_root,
        deploy_root=project_paths.bundle_root,
    )
    for artifact in artifacts:
        bundle_map.add(PathMapping(src=str(artifact.src), dest=artifact.dest))

    # Clean up bundle root
    project_paths.remove_up_bundle_root()

    for (absolute_src, absolute_dest) in bundle_map.all_mappings(
        absolute=True, expand_directories=True
    ):
        if absolute_src.is_file():
            # We treat the bundle/streamlit root as deploy root
            symlink_or_copy(
                absolute_src,
                absolute_dest,
                deploy_root=project_paths.bundle_root,
            )
            # Temporary solution, will be replaced with diff
            stage_path = (
                PurePosixPath(absolute_dest)
                .relative_to(project_paths.bundle_root)
                .parent
            )
            full_stage_path = f"{stage_root}/{stage_path}".rstrip("/")
            cli_console.step(f"Uploading {absolute_dest} to {full_stage_path}")
            stage_manager.put(
                local_path=absolute_dest, stage_path=full_stage_path, overwrite=True
            )
