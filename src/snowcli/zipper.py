import fnmatch
import os
from pathlib import Path
from typing import List
from zipfile import ZIP_DEFLATED, ZipFile

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
        myzip.write(file, os.path.basename(file))


def zip_current_dir(dest_zip: str) -> None:
    files_to_pack = _get_list_of_files_to_pack()
    files_to_pack = _filter_files(files_to_pack)
    _add_files_to_zip(dest_zip, files_to_pack)


def _get_list_of_files_to_pack() -> List[Path]:
    return [filepath.absolute() for filepath in Path(".").glob("**/*")]


def _filter_files(files: List[Path]) -> List[Path]:
    files_to_zip = []
    for file in files:
        file_name = file.__str__()

        if file.is_dir():
            continue

        for pattern in IGNORED_FILES:
            if file == pattern or fnmatch.fnmatch(file_name, pattern):
                break
        else:
            files_to_zip.append(file)

    return files_to_zip


def _add_files_to_zip(dest_zip: str, files_to_pack: List[Path]) -> None:
    with ZipFile(dest_zip, "w", ZIP_DEFLATED, allowZip64=True) as package_zip:
        for file in files_to_pack:
            package_zip.write(file, arcname=os.path.relpath(file, None))
