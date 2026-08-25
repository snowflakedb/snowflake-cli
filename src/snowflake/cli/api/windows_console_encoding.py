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

"""Detection of whether the Windows console attached to stdout is configured
for UTF-8 output.

This deliberately does not try to work out which shell (PowerShell, cmd.exe,
Git Bash, ...) launched the CLI — there is no reliable way to do that (e.g.
PSModulePath is set for cmd.exe children too), so callers get a single
generic signal instead and word their guidance to cover cmd.exe/Git Bash
(``chcp.com 65001``) and PowerShell 5.x (docs link) without claiming to know
which one is actually in use.
"""

from __future__ import annotations

import sys
from typing import Optional

from snowflake.cli.api.constants import IS_WINDOWS


def _console_output_codepage() -> Optional[int]:
    """Return the active Windows console output codepage, or None if
    unavailable (non-Windows, or no console attached — e.g. redirected).
    """
    if not IS_WINDOWS:
        return None
    try:
        from ctypes import windll  # type: ignore

        return windll.kernel32.GetConsoleOutputCP() or None
    except Exception:
        return None


def _console_needs_utf8_configuration() -> bool:
    """True when the console attached to stdout is on Windows, is a real
    TTY, and is not configured for UTF-8 output."""
    if not IS_WINDOWS:
        return False
    try:
        if not sys.stdout.isatty():
            return False
    except Exception:
        return False
    codepage = _console_output_codepage()
    # 65001 is the Windows codepage identifier for UTF-8.
    return codepage is not None and codepage != 65001
