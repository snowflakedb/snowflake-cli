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
"""Rich consoles backed by an encoded stream, not ``StringIO``."""

from __future__ import annotations

from io import BytesIO, TextIOWrapper

from rich.console import Console


def encoded_console(
    encoding: str,
    *,
    force_terminal: bool = False,
    width: int = 90,
) -> Console:
    stream = TextIOWrapper(BytesIO(), encoding=encoding, errors="strict", newline="\n")
    return Console(
        file=stream,
        force_terminal=force_terminal,
        width=width,
        no_color=True,
        markup=False,
    )


def cp1252_text_console(
    *,
    force_terminal: bool = False,
    width: int = 90,
) -> Console:
    return encoded_console("cp1252", force_terminal=force_terminal, width=width)


def written_bytes(console: Console) -> bytes:
    console.file.flush()
    return console.file.buffer.getvalue()
