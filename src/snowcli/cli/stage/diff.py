import re
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict

from snowflake.connector.cursor import SnowflakeCursor
from .manager import StageManager

MD5SUM_REGEX = r"^[A-Fa-f0-9]{32}$"
CHUNK_SIZE_BYTES = 8192


@dataclass
class DiffResult:
    """
    Each collection is a list of relative paths, either from
    the stage root or from the root of the compared local directory.
    """

    identical: List[str] = field(default_factory=list)
    "Files with matching md5sums"

    different: List[str] = field(default_factory=list)
    "Files that may be different between the stage and the local directory"

    only_local: List[str] = field(default_factory=list)
    "Files that only exist in the local directory"

    only_on_stage: List[str] = field(default_factory=list)
    "Files that only exist on the stage"


def is_valid_md5sum(checksum: str) -> bool:
    """
    Could the provided hexadecimal checksum represent a valid md5sum?
    """
    return re.match(MD5SUM_REGEX, checksum) is not None


def compute_md5sum(file: Path) -> str:
    """
    Returns a hexidecimal checksum for the file located at the given path.
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

    with open(file, "rb") as f:
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


def strip_stage_name(path: str) -> str:
    """Returns the given stage path without the stage name as the first part."""
    return "/".join(path.split("/")[1:])


def build_md5_map(list_stage_cursor: SnowflakeCursor) -> Dict[str, str]:
    """
    Returns a mapping of relative stage paths to their md5sums.
    """
    return {
        strip_stage_name(name): md5
        for (name, size, md5, modified) in list_stage_cursor.fetchall()
    }


def stage_diff(local_path: Path, stage_fqn: str) -> DiffResult:
    """
    Diffs the files in a stage with a local folder.
    """
    stage_manager = StageManager()
    local_files = enumerate_files(local_path)
    remote_md5 = build_md5_map(stage_manager.list(stage_fqn))

    result: DiffResult = DiffResult()

    for local_file in local_files:
        relpath = str(local_file.relative_to(local_path))
        if relpath not in remote_md5:
            # doesn't exist on the stage
            result.only_local.append(relpath)
        else:
            # N.B. we could compare local size vs remote size to skip the relatively-
            # expensive md5sum operation, but after seeing a comment that says the value
            # may not always be correctly populated, we'll ignore that column.
            stage_md5sum = remote_md5[relpath]
            if is_valid_md5sum(stage_md5sum) and stage_md5sum == compute_md5sum(
                local_file
            ):
                # the file definitely hasn't changed
                result.identical.append(relpath)
            else:
                # either the file has changed, or we can't tell if it has
                result.different.append(relpath)

            # mark this file as seen
            del remote_md5[relpath]

    # every entry here is a file we never saw locally
    for relpath in remote_md5.keys():
        result.only_on_stage.append(relpath)

    return result
