from __future__ import annotations

import os
from pathlib import Path

from snowflake.cli.api.artifacts.common import NotInDeployRootError
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils.path_utils import delete, resolve_without_follow


def symlink_or_copy(src: Path, dst: Path, deploy_root: Path) -> None:
    """
    Symlinks files from src to dst. If the src contains parent directories, then copies the empty directory shell to the deploy root.
    The directory hierarchy above dst is created if any of those directories do not exist.
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
        dst.mkdir(exist_ok=True)
        # 2. For all children of src, create their counterparts in dst now that it exists
        for root, _, files in sorted(os.walk(absolute_src, followlinks=True)):
            relative_root = Path(root).relative_to(absolute_src)
            absolute_root_in_deploy = Path(dst, relative_root)
            absolute_root_in_deploy.mkdir(parents=True, exist_ok=True)
            for file in sorted(files):
                absolute_file_in_project = Path(absolute_src, relative_root, file)
                absolute_file_in_deploy = Path(absolute_root_in_deploy, file)
                symlink_or_copy(
                    src=absolute_file_in_project,
                    dst=absolute_file_in_deploy,
                    deploy_root=deploy_root,
                )
