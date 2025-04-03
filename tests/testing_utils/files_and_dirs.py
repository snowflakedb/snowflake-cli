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

import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator, List, Optional, Union

from snowflake.cli.api.secure_utils import file_permissions_are_strict
from snowflake.cli.api.utils.path_utils import path_resolver


def create_temp_file(suffix: str, dir_name: str, contents: List[str]) -> str:
    with tempfile.NamedTemporaryFile(suffix=suffix, dir=dir_name, delete=False) as tmp:
        _write_to_file(tmp.name, contents)
    return tmp.name


def create_named_file(file_name: str, dir_name: str, contents: List[str]):
    file_path = os.path.join(dir_name, file_name)
    os.makedirs(dir_name, exist_ok=True)
    _write_to_file(file_path, contents)
    return file_path


def _write_to_file(path: str, contents: List[str]) -> None:
    with open(path, "w") as new_file:
        for line in contents:
            new_file.write(line + "\n")


def assert_file_permissions_are_strict(file_path: Path) -> None:
    assert file_permissions_are_strict(file_path)


@contextmanager
def temp_local_dir(
    dir_structure: Dict[str, Optional[Union[str, bytes]]]
) -> Generator[Path, None, None]:
    """
    Creates a temporary local directory structure from a dictionary
    of local paths and their file contents (either strings to be encoded
    as UTF-8, or binary bytes).

    Parameters:
     dir_structure (Dict[str, Optional[Union[str, bytes]]]): A dictionary of file or directory names along with their contents.
        For creating a file, 'contents' must be a string, empty or otherwise.
        For creating a directory, 'contents' must be set to None.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        for relpath, contents in dir_structure.items():
            path = Path(tmpdir, relpath)
            is_directory = contents is None
            if is_directory:
                path.mkdir(parents=True, exist_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                if contents is None:
                    f = open(path, "x")
                    f.close()
                else:
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


def resolve_path(path: Path):
    resolved = Path(path_resolver(str(path.absolute())))
    return resolved
