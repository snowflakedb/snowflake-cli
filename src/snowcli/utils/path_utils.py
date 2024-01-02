from __future__ import annotations

import sys

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
