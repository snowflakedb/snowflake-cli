import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator, List, Union

from snowflake.cli.api.secure_utils import file_permissions_are_strict


def create_temp_file(suffix: str, dir_name: str, contents: List[str]) -> str:
    with tempfile.NamedTemporaryFile(suffix=suffix, dir=dir_name, delete=False) as tmp:
        _write_to_file(tmp.name, contents)
    return tmp.name


def create_named_file(file_name: str, dir_name: str, contents: List[str]):
    file_path = os.path.join(dir_name, file_name)
    _write_to_file(file_path, contents)
    return file_path


def _write_to_file(path: str, contents: List[str]) -> None:
    with open(path, "w") as new_file:
        for line in contents:
            new_file.write(line + "\n")


def assert_file_permissions_are_strict(file_path: Path) -> None:
    assert file_permissions_are_strict(file_path)


@contextmanager
def temp_local_dir(files: Dict[str, Union[str, bytes]]) -> Generator[Path, None, None]:
    """
    Creates a temporary local directory structure from a dictionary
    of local paths and their file contents (either strings to be encoded
    as UTF-8, or binary bytes).
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        for relpath, contents in files.items():
            path = Path(tmpdir, relpath)
            path.parent.mkdir(parents=True, exist_ok=True)
            mode = "wb" if isinstance(contents, bytes) else "w"
            encoding = None if isinstance(contents, bytes) else "UTF-8"
            with open(path, mode=mode, encoding=encoding) as fh:
                fh.write(contents)

        yield Path(tmpdir)


def merge_left(target: Dict, source: Dict) -> None:
    """
    Recursively merges key/value pairs from source into target.
    Modifies the original dict-like "target".
    """
    for k, v in source.items():
        if k in target and isinstance(target[k], dict):
            # assumption: all inputs have been validated.
            merge_left(target[k], v)
        else:
            target[k] = v
