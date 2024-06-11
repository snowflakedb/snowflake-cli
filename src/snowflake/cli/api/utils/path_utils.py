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


def is_stage_path(path: str) -> bool:
    return path.startswith("@") or path.startswith("snow://")
