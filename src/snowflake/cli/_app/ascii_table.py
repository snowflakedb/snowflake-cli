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

import logging
import sys
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from rich.cells import cell_len, chop_cells
from rich.control import strip_control_codes
from snowflake.cli.api.sanitizers import sanitize_for_terminal

log = logging.getLogger(__name__)

PROBE_MAX_CELLS = 20_000
PROBE_MAX_CHARS = 2 * 1024 * 1024
PIPE_MAX_COLUMN_WIDTH = 256
WRITE_CHUNK_CHARS = 64 * 1024
COLUMN_FLOOR = 8
LARGE_RESULT_HINT = (
    "Large result: table output is streamed. Use --format csv for faster export."
)

_hint_emitted = False


class TableConsole(Protocol):
    width: int
    color_system: str | None
    no_color: bool


def normalize_cell(value: object) -> str:
    if isinstance(value, str):
        text = value
    elif isinstance(value, bytearray):
        text = value.hex()
    else:
        text = str(value)
    if text.isascii() and text.isprintable():
        return text
    sanitized = sanitize_for_terminal(text) or ""
    return strip_control_codes(sanitized).expandtabs(8)


class _Stop(Enum):
    CONTINUE = "continue"
    AT_LIMIT = "at"
    OVER = "over"


@dataclass(frozen=True)
class ProbeResult:
    columns: list[str]
    rows: list[list[str]]
    use_rich: bool
    pending: list[str] | None
    rest: Iterator[Mapping[str, object]]
    cells: int
    chars: int


def probe_rows(
    first: Mapping[str, object], rest: Iterator[Mapping[str, object]]
) -> ProbeResult:
    columns = [str(key) for key in first.keys()]
    rows: list[list[str]] = []
    cells = 0
    chars = 0
    pending: list[str] | None = None

    def consider(raw: Mapping[str, object]) -> _Stop:
        nonlocal cells, chars, pending
        normalized = [normalize_cell(value) for value in raw.values()]
        row_cells = len(normalized)
        row_chars = sum(len(cell) for cell in normalized)
        if rows and (
            cells + row_cells > PROBE_MAX_CELLS or chars + row_chars > PROBE_MAX_CHARS
        ):
            pending = normalized
            return _Stop.OVER
        rows.append(normalized)
        cells += row_cells
        chars += row_chars
        if cells > PROBE_MAX_CELLS or chars > PROBE_MAX_CHARS:
            return _Stop.OVER
        if cells >= PROBE_MAX_CELLS or chars >= PROBE_MAX_CHARS:
            return _Stop.AT_LIMIT
        return _Stop.CONTINUE

    status = consider(first)
    stopped = status is not _Stop.CONTINUE
    if not stopped:
        for raw in rest:
            status = consider(raw)
            if status is not _Stop.CONTINUE:
                stopped = True
                break

    use_rich = False
    if not stopped:
        use_rich = True
    elif status is _Stop.AT_LIMIT:
        try:
            peeked = next(rest)
        except StopIteration:
            use_rich = True
        else:
            pending = [normalize_cell(value) for value in peeked.values()]
            use_rich = False

    return ProbeResult(
        columns=columns,
        rows=rows,
        use_rich=use_rich,
        pending=pending,
        rest=rest,
        cells=cells,
        chars=chars,
    )


def preferred_widths(columns: list[str], rows: list[list[str]]) -> list[int]:
    widths = [1] * len(columns)
    for index, column in enumerate(columns):
        for line in column.split("\n"):
            widths[index] = max(widths[index], _content_width(line))
    for row in rows:
        for index, cell in enumerate(row):
            for line in cell.split("\n"):
                widths[index] = max(widths[index], _content_width(line))
    return widths


def _content_width(line: str) -> int:
    if not line:
        return 0
    if line.isascii():
        return len(line)
    return cell_len(line)


def allocate_tty_widths(preferred: list[int], console_width: int) -> list[int]:
    if not preferred:
        return []
    floors = [min(width, COLUMN_FLOOR) for width in preferred]
    available = console_width - (3 * len(preferred) + 1)
    widths = list(preferred)
    if sum(widths) <= available:
        return widths
    while True:
        excess = sum(widths) - available
        if excess <= 0:
            return widths
        shrinkable = [
            index for index, width in enumerate(widths) if width > floors[index]
        ]
        if not shrinkable:
            return widths
        widest = max(widths[index] for index in shrinkable)
        group = [index for index in shrinkable if widths[index] == widest]
        lower = [widths[index] for index in shrinkable if widths[index] < widest]
        next_lower = max(lower) if lower else 0
        drop_caps = [widths[index] - max(floors[index], next_lower) for index in group]
        step = min(drop_caps)
        if step <= 0:
            return widths
        if step * len(group) <= excess:
            for index in group:
                widths[index] -= step
            continue
        base, remainder = divmod(excess, len(group))
        for offset, index in enumerate(group):
            decrease = base + (1 if offset < remainder else 0)
            decrease = min(decrease, widths[index] - floors[index])
            widths[index] -= decrease
        return [max(width, 1) for width in widths]


def allocate_pipe_widths(preferred: list[int]) -> list[int]:
    return [min(width, PIPE_MAX_COLUMN_WIDTH) for width in preferred]


def split_cell(value: str, width: int, *, wrap: bool) -> list[str]:
    parts = value.split("\n")
    if not wrap:
        return parts or [""]
    lines: list[str] = []
    for part in parts:
        lines.extend(_fold_part(part, width))
    return lines or [""]


def _fold_part(part: str, width: int) -> list[str]:
    if width < 1:
        width = 1
    if part.isascii() and len(part) <= width:
        return [part]
    if cell_len(part) <= width:
        return [part]
    pieces = [piece for piece in chop_cells(part, width) if piece]
    return pieces or [part]


def pad_cell(piece: str, width: int) -> str:
    if piece.isascii() and "\n" not in piece and len(piece) <= width:
        return piece.ljust(width)
    size = cell_len(piece)
    if size >= width:
        return piece
    return piece + (" " * (width - size))


class _ChunkWriter:
    def __init__(self, stream: object, *, flush_chunks: bool) -> None:
        self._stream = stream
        self._flush_chunks = flush_chunks
        self._parts: list[str] = []
        self._size = 0

    def write(self, text: str) -> None:
        self._parts.append(text)
        self._size += len(text)
        if self._size >= WRITE_CHUNK_CHARS:
            self.flush()

    def flush(self, *, final: bool = False) -> None:
        if not self._parts:
            if final and self._flush_chunks:
                self._stream.flush()  # type: ignore[attr-defined]
            return
        text = "".join(self._parts)
        self._parts.clear()
        self._size = 0
        self._stream.write(text)  # type: ignore[attr-defined]
        if self._flush_chunks or final:
            self._stream.flush()  # type: ignore[attr-defined]


class _AsciiTableWriter:
    def __init__(
        self,
        *,
        columns: list[str],
        widths: list[int],
        wrap: bool,
        bold_header: bool,
        flush_chunks: bool,
    ) -> None:
        self._columns = columns
        self._widths = widths
        self._wrap = wrap
        self._bold_header = bold_header
        self._out = _ChunkWriter(sys.stdout, flush_chunks=flush_chunks)

    def write_table(
        self,
        probe_rows_values: list[list[str]],
        pending: list[str] | None,
        rest: Iterator[Mapping[str, object]],
        on_success: Callable[[], None],
    ) -> None:
        try:
            self._out.write(self._horizontal_rule())
            self._write_record(self._columns, bold=self._bold_header)
            self._out.write(self._header_rule())
            for row in probe_rows_values:
                self._write_record(row, bold=False)
            if pending is not None:
                self._write_record(pending, bold=False)
            for raw in rest:
                self._write_record(
                    [normalize_cell(value) for value in raw.values()],
                    bold=False,
                )
            self._out.write(self._horizontal_rule().rstrip("\n"))
            self._out.flush()
            on_success()
        finally:
            self._out.flush(final=True)

    def _write_record(self, values: list[str], *, bold: bool) -> None:
        if not bold and not self._wrap:
            plain = _format_plain_row(values, self._widths)
            if plain is not None:
                self._out.write(plain)
                return
        folded = [
            split_cell(value, width, wrap=self._wrap)
            for value, width in zip(values, self._widths)
        ]
        if not folded:
            self._out.write(self._format_line([], bold=bold) + "\n")
            return
        height = max(len(column) for column in folded)
        for line_index in range(height):
            pieces = [
                column[line_index] if line_index < len(column) else ""
                for column in folded
            ]
            self._out.write(self._format_line(pieces, bold=bold) + "\n")

    def _format_line(self, pieces: list[str], *, bold: bool) -> str:
        rendered: list[str] = []
        for piece, width in zip(pieces, self._widths):
            text = piece if _overflows(piece, width) else pad_cell(piece, width)
            if bold:
                text = f"\x1b[1m{text}\x1b[0m"
            rendered.append(text)
        return "| " + " | ".join(rendered) + " |"

    def _horizontal_rule(self) -> str:
        inner = (
            sum(self._widths) + 2 * len(self._widths) + max(len(self._widths) - 1, 0)
        )
        return "+" + ("-" * inner) + "+\n"

    def _header_rule(self) -> str:
        if not self._widths:
            return "|\n"
        body = "+".join("-" * (width + 2) for width in self._widths)
        return "|" + body + "|\n"


def _format_plain_row(values: list[str], widths: list[int]) -> str | None:
    cells: list[str] = []
    append = cells.append
    for piece, width in zip(values, widths):
        if "\n" in piece or not piece.isascii():
            return None
        if len(piece) <= width:
            append(piece.ljust(width))
        else:
            append(piece)
    return "| " + " | ".join(cells) + " |\n"


def _overflows(piece: str, width: int) -> bool:
    if piece.isascii() and "\n" not in piece:
        return len(piece) > width
    return cell_len(piece) > width


def stream_probed_table(
    probed: ProbeResult,
    *,
    console: TableConsole,
    is_tty: bool,
    on_success: Callable[[], None],
) -> None:
    preferred = preferred_widths(probed.columns, probed.rows)
    if is_tty:
        widths = allocate_tty_widths(preferred, console.width)
        wrap = True
    else:
        widths = allocate_pipe_widths(preferred)
        wrap = False
    bold_header = bool(is_tty and console.color_system and not console.no_color)
    writer = _AsciiTableWriter(
        columns=probed.columns,
        widths=widths,
        wrap=wrap,
        bold_header=bold_header,
        flush_chunks=is_tty,
    )
    writer.write_table(probed.rows, probed.pending, probed.rest, on_success)


def reset_large_result_hint() -> None:
    """Clear the once-per-process stderr hint so tests can observe it again."""
    global _hint_emitted
    _hint_emitted = False


def maybe_log_large_result(cells: int, chars: int, is_tty: bool) -> None:
    log.debug("streaming table output after probe cells=%s chars=%s", cells, chars)
    global _hint_emitted
    if is_tty and not _hint_emitted:
        _hint_emitted = True
        sys.stderr.write(LARGE_RESULT_HINT + "\n")
