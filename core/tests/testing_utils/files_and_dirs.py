import os
import tempfile
from typing import List


def create_temp_file(suffix: str, dir: str, contents: List[str]) -> str:
    with tempfile.NamedTemporaryFile(suffix=suffix, dir=dir, delete=False) as tmp:
        _write_to_file(tmp.name, contents)
    return tmp.name


def create_named_file(file_name: str, dir: str, contents: List[str]):
    file_path = os.path.join(dir, file_name)
    _write_to_file(file_path, contents)
    return file_path


def _write_to_file(path: str, contents: List[str]) -> None:
    with open(path, "w") as new_file:
        for line in contents:
            new_file.write(line + "\n")
