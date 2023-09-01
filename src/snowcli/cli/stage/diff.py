import re
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from snowcli.snow_connector import SnowflakeConnector, SnowflakeCursor
from .manager import StageManager

MD5SUM_REGEX = r"^[A-Fa-f0-9]{32}$"


@dataclass
class DiffResult:
    """
    Each collection is a list of relative paths, either from
    the stage root or from the root of the compared local directory.
    """

    unmodified: List[str] = field(default_factory=list)
    modified: List[str] = field(default_factory=list)
    only_local: List[str] = field(default_factory=list)
    only_on_stage: List[str] = field(default_factory=list)


def is_valid_md5sum(checksum: str) -> bool:
    """
    Could the provided hexadecimal checksum represent a valid md5sum?
    """
    return re.match(MD5SUM_REGEX, checksum)


def compute_md5sum(file: Path) -> str:
    """
    Returns a hexidecimal checksum for the file located at the given path.
    """
    if not file.is_file():
        raise ValueError(
            "The provided file does not exist or not a (symlink to a) regular file"
        )

    with open(file, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
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


def build_md5_map(list_stage_cursor: SnowflakeCursor) -> dict[str, str]:
    """
    Returns a mapping of relative stage paths to their md5sums.
    """
    # XXX: how can I get dicts back here?
    return {name: md5 for (name, size, md5, modified) in list_stage_cursor.fetchall()}


def stage_diff(local_path: Path, stage_fqn: str) -> DiffResult:
    stage_manager = StageManager()

    # N.B. we could compare local size vs remote size, but after seeing a comment
    # that says the value may not be correctly populated, we'll just skip it.

    local_files = enumerate_files(local_path)
    remote_md5 = build_md5_map(stage_manager.list(stage_fqn))

    result: DiffResult = DiffResult()

    for local_file in local_files:
        relpath = str(local_file.relative_to(local_path))
        if relpath not in remote_md5:
            # doesn't exist on the stage
            result.only_local.append(relpath)
        else:
            stage_md5sum = remote_md5[relpath]
            if is_valid_md5sum() and stage_md5sum == compute_md5sum(local_file):
                # the file definitely hasn't changed
                result.unmodified.append(relpath)
            else:
                # either the file has changed, or we can't tell if it has
                result.modified.append(relpath)

            # mark this file as seen
            del remote_md5[relpath]

    # every entry here is a file we never saw locally
    for relpath in remote_md5.keys():
        result.only_on_stage.append(relpath)

    return result
