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

from __future__ import annotations

import fnmatch
import logging
from pathlib import Path
from typing import Dict, List, Literal
from zipfile import ZIP_DEFLATED, ZipFile

from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

IGNORED_FILES = [
    "**/.DS_Store",
    "**/.git",
    "**/.git/*",
    "**/.gitignore",
    "**/.env",
    "**/.env/*",
    "**/.venv",
    "**/.venv/*",
    "**/__pycache__",
    "**/*.zip",
    "**/*.pyc",
    "**/env",
    "**/env/*",
    "**/ENV",
    "**/ENV/*",
    "**/venv",
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
    source: Path | List[Path],
    dest_zip: Path,
    mode: Literal["r", "w", "x", "a"] = "w",
) -> None:

    if not dest_zip.parent.exists():
        SecurePath(dest_zip).parent.mkdir(parents=True)

    if isinstance(source, Path) or isinstance(source, SecurePath):
        source = [source]

    files_to_pack: Dict[Path, List[Path]] = {
        src: list(filter(_to_be_zipped, (f.absolute() for f in src.glob("**/*"))))
        for src in source
    }

    with ZipFile(dest_zip, mode, ZIP_DEFLATED, allowZip64=True) as package_zip:
        for src, files in files_to_pack.items():
            if isinstance(src, SecurePath):
                src = src.path
            for file in files:
                log.debug("Adding %s to %s", file, dest_zip)
                package_zip.write(file, arcname=file.relative_to(src))


def zip_dir_using_bundle_map(
    bundle_map: BundleMap,
    dest_zip: Path,
    mode: Literal["r", "w", "x", "a"] = "w",
) -> None:
    if not dest_zip.parent.exists():
        SecurePath(dest_zip).parent.mkdir(parents=True)

    with ZipFile(dest_zip, mode, ZIP_DEFLATED, allowZip64=True) as package_zip:
        cli_console.step(f"Creating: {dest_zip}")
        for src, _ in bundle_map.all_mappings(expand_directories=True):
            if src.is_file():
                log.debug("Adding %s to %s", src, dest_zip)
                package_zip.write(src, arcname=_path_without_top_level_directory(src))


def _path_without_top_level_directory(path: Path) -> str:
    path_parts = path.parts
    if len(path_parts) > 1:
        return str(Path(*path_parts[1:]))
    return str(path)


def _to_be_zipped(file: Path) -> bool:
    for pattern in IGNORED_FILES:
        # This has to be a string because of fnmatch
        file_as_str = str(file)
        if file_as_str == pattern or fnmatch.fnmatch(file_as_str, pattern):
            return False

    return True
