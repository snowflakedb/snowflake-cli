import hashlib
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Collection, Dict, List, Optional

from snowflake.cli.api.exceptions import (
    SnowflakeSQLExecutionError,
)
from snowflake.cli.api.secure_path import UNLIMITED, SecurePath
from snowflake.connector.cursor import DictCursor

from .manager import StageManager

MD5SUM_REGEX = r"^[A-Fa-f0-9]{32}$"
CHUNK_SIZE_BYTES = 8192

log = logging.getLogger(__name__)

StagePath = PurePosixPath  # alias PurePosixPath as StagePath for clarity


@dataclass
class DiffResult:
    """
    Each collection is a list of stage paths ('/'-separated, regardless of the platform), relative to the stage root.
    """

    identical: List[StagePath] = field(default_factory=list)
    "Files with matching md5sums"

    different: List[StagePath] = field(default_factory=list)
    "Files that may be different between the stage and the local directory"

    only_local: List[StagePath] = field(default_factory=list)
    "Files that only exist in the local directory"

    only_on_stage: List[StagePath] = field(default_factory=list)
    "Files that only exist on the stage"

    def has_changes(self) -> bool:
        return (
            len(self.different) > 0
            or len(self.only_local) > 0
            or len(self.only_on_stage) > 0
        )

    def __str__(self) -> str:
        """
        Method override for the standard behavior of string representation for this class.
        """
        components: List[
            str
        ] = (
            []
        )  # py3.8 does not support subscriptions for builtin list, hence using List

        # The specific order of conditionals is for an aesthetically pleasing output and ease of readability.
        if not self.only_local:
            components.append(
                "There are no new files that exist only in your local directory."
            )
        if not self.only_on_stage:
            components.append("There are no new files that exist only on the stage.")
        if not self.different:
            components.append(
                "There are no existing files that have been modified, or their status is unknown."
            )
        if not self.identical:
            components.append(
                "There are no existing files that are identical to the ones on the stage."
            )

        if self.only_local:
            components.extend(
                ["New files only on your local:", *[str(p) for p in self.only_local]]
            )
        if self.only_on_stage:
            components.extend(
                ["New files only on the stage:", *[str(p) for p in self.only_on_stage]]
            )
        if self.different:
            components.extend(
                [
                    "Existing files modified or status unknown:",
                    *[str(p) for p in self.different],
                ]
            )
        if self.identical:
            components.extend(
                [
                    "Existing files identical to the stage:",
                    *[str(p) for p in self.identical],
                ]
            )

        return "\n".join(components)


def is_valid_md5sum(checksum: str) -> bool:
    """
    Could the provided hexadecimal checksum represent a valid md5sum?
    """
    return re.match(MD5SUM_REGEX, checksum) is not None


def compute_md5sum(file: Path) -> str:
    """
    Returns a hexadecimal checksum for the file located at the given path.
    """
    if not file.is_file():
        raise ValueError(
            "The provided file does not exist or not a (symlink to a) regular file"
        )

    # FIXME: there are two cases in which this will fail to provide a matching
    # md5sum, even when the underlying file is the same:
    #  1. when the stage uses SNOWFLAKE_FULL encryption
    #  2. when the file was uploaded in multiple parts

    # We can re-create the second if we know what chunk size was used by the
    # upload process to the backing object store (e.g. S3, azure blob, etc.)
    # but we cannot re-create the first as the encrpytion key is hidden.

    # We are assuming that we will not get accidental collisions here due to the
    # large space of the md5sum (32 * 4 = 128 bits means 1-in-9-trillion chance)
    # combined with the fact that the file name + path must also match elsewhere.

    with SecurePath(file).open("rb", read_file_limit_mb=UNLIMITED) as f:
        file_hash = hashlib.md5()
        while chunk := f.read(CHUNK_SIZE_BYTES):
            file_hash.update(chunk)

    return file_hash.hexdigest()


def enumerate_files(path: Path) -> List[Path]:
    """
    Get a list of all files in a directory (recursively).
    """
    if not path.is_dir():
        raise ValueError("Path must point to a directory")

    paths: List[Path] = []
    for child in sorted(path.iterdir()):
        if child.is_dir():
            paths += enumerate_files(child)
        else:
            paths.append(child)

    return paths


def strip_stage_name(path: str) -> StagePath:
    """Returns the given stage path without the stage name as the first part."""
    return StagePath(*path.split("/")[1:])


def build_md5_map(list_stage_cursor: DictCursor) -> Dict[StagePath, str]:
    """
    Returns a mapping of relative stage paths to their md5sums.
    """
    return {
        strip_stage_name(file["name"]): file["md5"]
        for file in list_stage_cursor.fetchall()
    }


def preserve_from_diff(
    diff: DiffResult, stage_paths_to_sync: Collection[StagePath]
) -> DiffResult:
    """
    Returns a filtered version of the provided diff, keeping only the provided stage paths.
    """
    paths_to_preserve = set(stage_paths_to_sync)
    preserved_diff: DiffResult = DiffResult()
    preserved_diff.identical = [i for i in diff.identical if i in paths_to_preserve]
    preserved_diff.different = [i for i in diff.different if i in paths_to_preserve]
    preserved_diff.only_local = [i for i in diff.only_local if i in paths_to_preserve]
    preserved_diff.only_on_stage = [
        i for i in diff.only_on_stage if i in paths_to_preserve
    ]
    return preserved_diff


def compute_stage_diff(
    local_root: Path,
    stage_fqn: str,
) -> DiffResult:
    """
    Diffs the files in a stage with a local folder.
    """
    stage_manager = StageManager()
    local_files = enumerate_files(local_root)
    remote_md5 = build_md5_map(stage_manager.list_files(stage_fqn))

    result: DiffResult = DiffResult()

    for local_file in local_files:
        relpath = local_file.relative_to(local_root)
        stage_filename = to_stage_path(relpath)
        if stage_filename not in remote_md5:
            # doesn't exist on the stage
            result.only_local.append(stage_filename)
        else:
            # N.B. we could compare local size vs remote size to skip the relatively-
            # expensive md5sum operation, but after seeing a comment that says the value
            # may not always be correctly populated, we'll ignore that column.
            stage_md5sum = remote_md5[stage_filename]
            if is_valid_md5sum(stage_md5sum) and stage_md5sum == compute_md5sum(
                local_file
            ):
                # the file definitely hasn't changed
                result.identical.append(stage_filename)
            else:
                # either the file has changed, or we can't tell if it has
                result.different.append(stage_filename)

            # mark this file as seen
            del remote_md5[stage_filename]

    # every entry here is a file we never saw locally
    for stage_filename in remote_md5.keys():
        result.only_on_stage.append(stage_filename)

    return result


def get_stage_subpath(stage_path: StagePath) -> str:
    """
    Returns the parent portion of a stage path, as a string, for inclusion in the fully qualified stage path. Note that
    '.' treated specially here, and so the return value of this call is not a `StagePath` instance.
    """
    parent = str(stage_path.parent)
    return "" if parent == "." else parent


def to_stage_path(filename: Path) -> StagePath:
    """
    Returns the stage file name, with the path separator suitably transformed if needed.
    """
    return StagePath(*filename.parts)


def to_local_path(stage_path: StagePath) -> Path:
    return Path(*stage_path.parts)


def delete_only_on_stage_files(
    stage_manager: StageManager,
    stage_fqn: str,
    only_on_stage: List[StagePath],
    role: Optional[str] = None,
):
    """
    Deletes all files from a Snowflake stage according to the input list of filenames, using a custom role.
    """
    for _stage_filename in only_on_stage:
        stage_manager.remove(stage_name=stage_fqn, path=str(_stage_filename), role=role)


def put_files_on_stage(
    stage_manager: StageManager,
    stage_fqn: str,
    deploy_root_path: Path,
    stage_paths: List[StagePath],
    role: Optional[str] = None,
    overwrite: bool = False,
):
    """
    Uploads all files given input list of filenames on your local filesystem, to a Snowflake stage, using a custom role.
    """
    for _stage_path in stage_paths:
        stage_sub_path = get_stage_subpath(_stage_path)
        full_stage_path = (
            f"{stage_fqn}/{stage_sub_path}" if stage_sub_path else stage_fqn
        )
        stage_manager.put(
            local_path=deploy_root_path / to_local_path(_stage_path),
            stage_path=full_stage_path,
            role=role,
            overwrite=overwrite,
        )


def sync_local_diff_with_stage(
    role: str, deploy_root_path: Path, diff_result: DiffResult, stage_fqn: str
):
    """
    Syncs a given local directory's contents with a Snowflake stage, including removing old files, and re-uploading modified and new files.
    """
    stage_manager = StageManager()
    log.info(
        "Uploading diff-ed files from your local %s directory to the Snowflake stage.",
        deploy_root_path,
    )

    try:
        delete_only_on_stage_files(
            stage_manager, stage_fqn, diff_result.only_on_stage, role
        )
        put_files_on_stage(
            stage_manager,
            stage_fqn,
            deploy_root_path,
            diff_result.different,
            role,
            overwrite=True,
        )
        put_files_on_stage(
            stage_manager, stage_fqn, deploy_root_path, diff_result.only_local, role
        )
    except Exception as err:
        # Could be ProgrammingError or IntegrityError from SnowflakeCursor
        log.error(err)
        raise SnowflakeSQLExecutionError()
