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
"""ASCII stand-ins for DCM glyphs when stdout is not UTF."""

from __future__ import annotations

from typing import Optional

from rich import get_console
from rich.console import Console
from rich.spinner import Spinner

_RUNNING_FRAME = Spinner("dots").frames[0]


def ascii_only(console: Optional[Console] = None) -> bool:
    encoding_console = console if console is not None else get_console()
    return encoding_console.options.ascii_only


def pick(
    unicode_glyph: str, ascii_glyph: str, console: Optional[Console] = None
) -> str:
    """*ascii_glyph* when *console* (default: ``get_console()``) is not UTF."""
    if ascii_only(console):
        return ascii_glyph
    return unicode_glyph


def warning_mark(console: Optional[Console] = None) -> str:
    return pick("⚠️", "!", console)


def pass_mark(console: Optional[Console] = None) -> str:
    return pick("✓", "v", console)


def fail_mark(console: Optional[Console] = None) -> str:
    return pick("✗", "x", console)


def tree_leaf(console: Optional[Console] = None) -> str:
    return pick("└─", "`-", console)


def bullet(console: Optional[Console] = None) -> str:
    return pick("• ", "* ", console)


def ellipsis(console: Optional[Console] = None) -> str:
    return pick("…", "...", console)


def arrow(console: Optional[Console] = None) -> str:
    return pick("→", "->", console)


def pending_mark(console: Optional[Console] = None) -> str:
    return pick("·", ".", console)


def running_mark(console: Optional[Console] = None) -> str:
    return pick(_RUNNING_FRAME, "*", console)


def step_prefix(console: Optional[Console] = None) -> str:
    return pick("❯", ">", console)
