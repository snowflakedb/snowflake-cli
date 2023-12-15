import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator, List, Union


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
