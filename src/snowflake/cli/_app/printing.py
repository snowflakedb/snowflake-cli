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

import json
import sys
from datetime import date, datetime
from json import JSONEncoder
from pathlib import Path
from textwrap import indent
from typing import TextIO

from rich import box, get_console
from rich import print as rich_print
from rich.live import Live
from rich.table import Table
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    MultipleResults,
    ObjectResult,
    StreamResult,
)
from snowflake.cli.api.sanitizers import sanitize_for_terminal

NO_ITEMS_FOUND: str = "No data"

# ensure we do not break URLs that wrap lines
get_console().soft_wrap = True

# Disable markup to avoid escaping errors
get_console()._markup = False  # noqa: SLF001


class CustomJSONEncoder(JSONEncoder):
    """Custom JSON encoder handling serialization of non-standard types"""

    def default(self, o):
        if isinstance(o, str):
            return sanitize_for_terminal(o)
        if isinstance(o, (ObjectResult, MessageResult)):
            return o.result
        if isinstance(o, (CollectionResult, MultipleResults)):
            return list(o.result)
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        if isinstance(o, Path):
            return str(o)
        return super().default(o)


def _get_format_type() -> OutputFormat:
    output_format = get_cli_context().output_format
    if output_format:
        return output_format
    return OutputFormat.TABLE


def _get_table():
    return Table(show_header=True, box=box.ASCII)


def _print_multiple_table_results(obj: CollectionResult):
    items = obj.result
    try:
        first_item = next(items)
    except StopIteration:
        rich_print(NO_ITEMS_FOUND, end="\n\n")
        return
    table = _get_table()
    for column in first_item.keys():
        table.add_column(column, overflow="fold")
    with Live(table, refresh_per_second=4):
        table.add_row(*[str(i) for i in first_item.values()])
        for item in items:
            table.add_row(*[str(i) for i in item.values()])
    # Add separator between tables
    rich_print(flush=True)


def is_structured_format(output_format):
    return output_format == OutputFormat.JSON


def print_structured(result: CommandResult):
    """Handles outputs like json, yml and other structured and parsable formats."""
    printed_end_line = False
    if isinstance(result, MultipleResults):
        _stream_json(result)
    elif isinstance(result, StreamResult):
        # A StreamResult prints each value onto its own line
        # instead of joining all the values into a JSON array
        for r in result.result:
            json.dump(r, sys.stdout, cls=CustomJSONEncoder)
            print(flush=True)
            printed_end_line = True
    else:
        json.dump(result, sys.stdout, cls=CustomJSONEncoder, indent=4)
    # Adds empty line at the end
    if not printed_end_line:
        print(flush=True)


def _stream_json(result):
    """Simple helper for streaming multiple results as a JSON."""
    indent_size = 2

    class _Indented:
        def __init__(self, stream: TextIO):
            self._stream = stream

        def write(self, text: str):
            return self._stream.write(indent(text, " " * indent_size))

    print("[")
    results = result.result
    res = next(results, None)
    while res:
        json.dump(res, _Indented(sys.stdout), cls=CustomJSONEncoder, indent=indent_size)  # type: ignore
        if res := next(results, None):
            print(",")
    print("\n]")


def print_unstructured(obj: CommandResult | None):
    """Handles outputs like table, plain text and other unstructured types."""
    if not obj:
        rich_print("Done", flush=True)
    elif not obj.result:
        rich_print("No data", flush=True)
    elif isinstance(obj, MessageResult):
        rich_print(sanitize_for_terminal(obj.message), flush=True)
    else:
        if isinstance(obj, ObjectResult):
            _print_single_table(obj)
        elif isinstance(obj, CollectionResult):
            _print_multiple_table_results(obj)
        else:
            raise TypeError(f"No print strategy for type: {type(obj)}")


def _print_single_table(obj):
    table = _get_table()
    table.add_column("key", overflow="fold")
    table.add_column("value", overflow="fold")
    for key, value in obj.result.items():
        table.add_row(
            sanitize_for_terminal(str(key)), sanitize_for_terminal(str(value))
        )
    rich_print(table, flush=True)


def print_result(cmd_result: CommandResult, output_format: OutputFormat | None = None):
    output_format = output_format or _get_format_type()
    if is_structured_format(output_format):
        print_structured(cmd_result)
    elif isinstance(cmd_result, (MultipleResults, StreamResult)):
        for res in cmd_result.result:
            print_result(res)
    elif (
        isinstance(cmd_result, (MessageResult, ObjectResult, CollectionResult))
        or cmd_result is None
    ):
        print_unstructured(cmd_result)
    else:
        raise ValueError(f"Unexpected type {type(cmd_result)}")
