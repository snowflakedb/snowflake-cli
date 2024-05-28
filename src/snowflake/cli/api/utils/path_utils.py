from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Union

BUFFER_SIZE = 4096


def path_resolver(path_to_file: str) -> str:
    if sys.platform == "win32" and "~1" in path_to_file:
        from ctypes import create_unicode_buffer, windll  # type: ignore

        buffer = create_unicode_buffer(BUFFER_SIZE)
        get_long_path_name = windll.kernel32.GetLongPathNameW
        return_value = get_long_path_name(path_to_file, buffer, BUFFER_SIZE)

        if 0 < return_value <= BUFFER_SIZE:
            return buffer.value
    return path_to_file


def is_stage_path(path: str) -> bool:
    return path.startswith("@") or path.startswith("snow://")


def safe_rmtree(path: Union[Path, str]):
    def _remove_readonly(func, _path, _):
        "Clear the readonly bit and reattempt the removal"
        os.chmod(_path, os.stat.S_IWRITE)
        func(_path)

    shutil.rmtree(str(path), onerror=_remove_readonly)
