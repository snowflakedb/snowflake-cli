from __future__ import annotations

import fnmatch
import logging
from pathlib import Path
from typing import Iterator, Literal
from zipfile import ZIP_DEFLATED, ZipFile

log = logging.getLogger(__name__)

IGNORED_FILES = [
    "**/.DS_Store",
    "**/.git/*",
    "**/.gitignore",
    "**/.env/*",
    "**/.venv/*",
    "**/__pycache__",
    "**/*.zip",
    "**/*.pyc",
    "**/env/*",
    "**/ENV/*",
    "**/venv/*",
    "**/requirements.txt",
    "**/requirements.snowflake.txt",
    "**/requirements.other.txt",
    "**/snowflake.yml",
]


def add_file_to_existing_zip(zip_file: str, file: str):
    """Adds another file to an existing zip file

    Args:
        zip_file (str): The existing zip file
        file (str): The new file to add
    """
    with ZipFile(zip_file, mode="a") as myzip:
        myzip.write(file, Path(file).name)


def zip_dir(
    source: Path, dest_zip: Path, mode: Literal["r", "w", "x", "a"] = "w"
) -> None:
    files_to_pack: Iterator[Path] = filter(
        _to_be_zipped, map(lambda f: f.absolute(), source.glob("**/*"))
    )

    with ZipFile(dest_zip, mode, ZIP_DEFLATED, allowZip64=True) as package_zip:
        for file in files_to_pack:
            log.debug("Adding %s to %s", file, dest_zip)
            package_zip.write(file, arcname=file.relative_to(source.absolute()))


def _to_be_zipped(file: Path) -> bool:
    if file.is_dir():
        return False

    for pattern in IGNORED_FILES:
        # This has to be a string because of fnmatch
        file_as_str = str(file)
        if file_as_str == pattern or fnmatch.fnmatch(file_as_str, pattern):
            return False

    return True
