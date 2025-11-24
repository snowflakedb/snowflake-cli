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

import re
from typing import List

# 7-bit C1 ANSI sequences
_ANSI_ESCAPE = re.compile(
    r"""
    \x1B  # ESC
    (?:   # 7-bit C1 Fe (except CSI)
        [@-Z\\-_]
    |     # or [ for CSI, followed by a control sequence
        \[
        [0-?]*  # Parameter bytes
        [ -/]*  # Intermediate bytes
        [@-~]   # Final byte
    )
""",
    re.VERBOSE,
)


def sanitize_for_terminal(text: str) -> str | None:
    """
    Escape ASCII escape codes in string. This should be always used
    when printing output to terminal.
    """
    if text is None:
        return None
    return _ANSI_ESCAPE.sub("", text)


def sanitize_source_error(exc: Exception) -> str:
    """
    Produce a logging-safe description of discovery errors.

    Keys and structural metadata (section/key/line) are preserved, but raw
    values are never rendered so sensitive data cannot leak through logs.
    """

    safe_parts: List[str] = [exc.__class__.__name__]
    attribute_labels = (
        ("section", "section"),
        ("option", "key"),
        ("key", "key"),
        ("lineno", "line"),
        ("colno", "column"),
        ("pos", "position"),
    )

    for attr_name, label in attribute_labels:
        attr_value = getattr(exc, attr_name, None)
        if attr_value:
            safe_parts.append(f"{label}={attr_value}")

    if len(safe_parts) == 1:
        safe_parts.append("details_masked")

    return ", ".join(safe_parts)
