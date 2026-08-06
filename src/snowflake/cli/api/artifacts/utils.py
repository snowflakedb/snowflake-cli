from __future__ import annotations

import os
from pathlib import Path

from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.artifacts.common import NotInDeployRootError
from snowflake.cli.api.constants import PatternMatchingType
from snowflake.cli.api.project.project_paths import ProjectPaths
from snowflake.cli.api.project.schemas.entities.common import Artifacts
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils.path_utils import delete, resolve_without_follow


def symlink_or_copy(
    src: Path,
    dst: Path,
    deploy_root: Path,
    project_root: Path | None = None,
) -> None:
    """
    Symlinks files from src to dst. If the src contains parent directories, then copies the empty directory shell to the deploy root.
    The directory hierarchy above dst is created if any of those directories do not exist.

    When ``project_root`` is provided, any directory or file whose real
    (symlink-resolved) target escapes the project root is pruned during
    recursive traversal of a directory ``src``. This prevents a committed
    symlink (e.g. ``project/data -> /etc``) from leaking files outside the
    project into the deploy root and, subsequently, onto a Snowflake stage.
    """
    ssrc = SecurePath(src)
    sdst = SecurePath(dst)
    sdst.parent.mkdir(parents=True, exist_ok=True)

    # Verify that the mapping isn't accidentally trying to create a file in the project source through symlinks.
    # We need to ensure we're resolving symlinks for this check to be effective.
    # We are unlikely to hit this if calling the function through bundle map, keeping it here for other future use cases outside bundle.
    resolved_dst = dst.resolve()
    resolved_deploy_root = deploy_root.resolve()
    dst_is_deploy_root = resolved_deploy_root == resolved_dst
    if (not dst_is_deploy_root) and (resolved_deploy_root not in resolved_dst.parents):
        raise NotInDeployRootError(dest_path=dst, deploy_root=deploy_root, src_path=src)

    absolute_src = resolve_without_follow(src)
    if absolute_src.is_file():
        delete(dst)
        try:
            os.symlink(absolute_src, dst)
        except OSError:
            ssrc.copy(dst)
    else:
        # 1. Create a new directory in the deploy root
        sdst.mkdir(exist_ok=True)
        # Prune any directory or file whose realpath escapes the project root,
        # so a committed symlink (e.g. ``src/escape -> /etc``) nested inside an
        # otherwise-legitimate directory source is skipped rather than followed
        # into the host filesystem during bundling.
        real_root = (
            Path(os.path.realpath(project_root)) if project_root is not None else None
        )

        def _stays_in_real_project_root(p: Path) -> bool:
            if real_root is None:
                return True
            try:
                real = Path(os.path.realpath(p))
            except OSError:
                return False
            return real == real_root or real_root in real.parents

        # 2. For all children of src, create their counterparts in dst now that it exists
        for root, dirs, files in sorted(os.walk(absolute_src, followlinks=True)):
            dirs[:] = [d for d in dirs if _stays_in_real_project_root(Path(root) / d)]
            files = [f for f in files if _stays_in_real_project_root(Path(root) / f)]
            relative_root = Path(root).relative_to(absolute_src)
            absolute_root_in_deploy = Path(dst, relative_root)
            SecurePath(absolute_root_in_deploy).mkdir(parents=True, exist_ok=True)
            for file in sorted(files):
                absolute_file_in_project = Path(absolute_src, relative_root, file)
                absolute_file_in_deploy = Path(absolute_root_in_deploy, file)
                symlink_or_copy(
                    src=absolute_file_in_project,
                    dst=absolute_file_in_deploy,
                    deploy_root=deploy_root,
                    project_root=project_root,
                )


def bundle_artifacts(
    project_paths: ProjectPaths,
    artifacts: Artifacts,
    pattern_type: PatternMatchingType = PatternMatchingType.GLOB,
) -> BundleMap:
    """
    Creates a bundle directory (project_paths.bundle_root) with all artifacts (using symlink_or_copy function above).
    Previous contents of the directory are deleted.

    Returns a BundleMap containing the mapping between artifacts and their location in bundle directory.

    Args:
        project_paths: Project paths configuration
        artifacts: List of artifacts to bundle
        pattern_type: The pattern matching type to use for artifact resolution. Defaults to GLOB.
    """
    bundle_map = BundleMap(
        project_root=project_paths.project_root,
        deploy_root=project_paths.bundle_root,
        pattern_type=pattern_type,
    )
    for artifact in artifacts:
        bundle_map.add(artifact)

    for absolute_src, absolute_dest in bundle_map.all_mappings(
        absolute=True, expand_directories=True
    ):
        if absolute_src.is_file():
            # We treat the bundle root as deploy root
            symlink_or_copy(
                absolute_src,
                absolute_dest,
                deploy_root=project_paths.bundle_root,
                project_root=project_paths.project_root,
            )

    return bundle_map
