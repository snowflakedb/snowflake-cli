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

import csv
import json
import sys
from datetime import date, datetime, time
from decimal import Decimal
from json import JSONEncoder
from pathlib import Path
from textwrap import indent
from typing import Any, Dict, TextIO

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
        if isinstance(o, (date, datetime, time)):
            return o.isoformat()
        if isinstance(o, Path):
            return o.as_posix()
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, bytearray):
            return o.hex()
        return super().default(o)


class StreamingJSONEncoder(JSONEncoder):
    """Streaming JSON encoder that doesn't materialize generators into lists"""

    def default(self, o):
        if isinstance(o, str):
            return sanitize_for_terminal(o)
        if isinstance(o, (ObjectResult, MessageResult)):
            return o.result
        if isinstance(o, (CollectionResult, MultipleResults)):
            raise TypeError(
                f"CollectionResult should be handled by streaming functions, not encoder"
            )
        if isinstance(o, (date, datetime, time)):
            return o.isoformat()
        if isinstance(o, Path):
            return o.as_posix()
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, bytearray):
            return o.hex()
        return super().default(o)


def _print_json_item_with_array_indentation(item: Any, indent: int):
    """Print a JSON item with proper indentation for array context"""
    if indent:
        indented_output = json.dumps(item, cls=StreamingJSONEncoder, indent=indent)
        indented_lines = indented_output.split("\n")
        for i, line in enumerate(indented_lines):
            if i == 0:
                print(" " * indent + line, end="")
            else:
                print("\n" + " " * indent + line, end="")
    else:
        json.dump(item, sys.stdout, cls=StreamingJSONEncoder, separators=(",", ":"))


def _stream_collection_as_json(result: CollectionResult, indent: int = 4):
    """Stream a CollectionResult as a JSON array without loading all data into memory"""
    items = iter(result.result)
    try:
        first_item = next(items)
    except StopIteration:
        print("[]", end="")
        return

    print("[")

    _print_json_item_with_array_indentation(first_item, indent)

    for item in items:
        print(",")
        _print_json_item_with_array_indentation(item, indent)

    print("\n]", end="")


def _stream_collection_as_csv(result: CollectionResult):
    """Stream a CollectionResult as CSV without loading all data into memory"""
    items = iter(result.result)
    try:
        first_item = next(items)
    except StopIteration:
        return

    fieldnames = list(first_item.keys())
    if not isinstance(first_item, dict):
        raise TypeError("CSV output requires dictionary items")

    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    _write_csv_row(writer, first_item)

    for item in items:
        _write_csv_row(writer, item)


def _write_csv_row(writer: csv.DictWriter, row_data: Dict[str, Any]):
    """Write a single CSV row, handling special data types"""
    processed_row = {}
    for key, value in row_data.items():
        if isinstance(value, str):
            processed_row[key] = sanitize_for_terminal(value)
        elif isinstance(value, (date, datetime, time)):
            processed_row[key] = value.isoformat()
        elif isinstance(value, Path):
            processed_row[key] = value.as_posix()
        elif isinstance(value, Decimal):
            processed_row[key] = str(value)
        elif isinstance(value, bytearray):
            processed_row[key] = value.hex()
        elif value is None:
            processed_row[key] = ""
        else:
            processed_row[key] = str(value)

    writer.writerow(processed_row)


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
        table.add_row(*[__to_str(i) for i in first_item.values()])
        for item in items:
            table.add_row(*[__to_str(i) for i in item.values()])
    # Add separator between tables
    rich_print(flush=True)


def __to_str(val):
    if isinstance(val, bytearray):
        return val.hex()
    return str(val)


def is_structured_format(output_format):
    return output_format.is_json or output_format == OutputFormat.CSV


def print_structured(
    result: CommandResult, output_format: OutputFormat = OutputFormat.JSON
):
    """Handles outputs like json, csv and other structured and parsable formats with streaming."""
    printed_end_line = False

    if isinstance(result, MultipleResults):
        if output_format == OutputFormat.CSV:
            for command_result in result.result:
                _print_csv_result_streaming(command_result)
                print(flush=True)
            printed_end_line = True
        else:
            _stream_json(result)
    elif isinstance(result, StreamResult):
        # A StreamResult prints each value onto its own line
        # instead of joining all the values into a JSON array or CSV entry set
        for r in result.result:
            if output_format == OutputFormat.CSV:
                _print_csv_result_streaming(r)
            else:
                json.dump(r, sys.stdout, cls=StreamingJSONEncoder)
            print(flush=True)
            printed_end_line = True
    else:
        if output_format == OutputFormat.CSV:
            _print_csv_result_streaming(result)
            printed_end_line = True
        else:
            _print_json_result_streaming(result)

    # Adds empty line at the end
    if not printed_end_line:
        print(flush=True)


def _print_json_result_streaming(result: CommandResult):
    """Print a single CommandResult as JSON with streaming support"""
    if isinstance(result, CollectionResult):
        _stream_collection_as_json(result, indent=4)
    elif isinstance(result, (ObjectResult, MessageResult)):
        json.dump(result, sys.stdout, cls=StreamingJSONEncoder, indent=4)
    else:
        json.dump(result, sys.stdout, cls=StreamingJSONEncoder, indent=4)


def _print_object_result_as_csv(result: ObjectResult):
    """Print an ObjectResult as a single-row CSV.

    Converts the object's key-value pairs into a CSV with headers
    from the keys and a single data row from the values.
    """
    data = result.result
    if isinstance(data, dict):
        writer = csv.DictWriter(
            sys.stdout, fieldnames=list(data.keys()), lineterminator="\n"
        )
        writer.writeheader()
        _write_csv_row(writer, data)


def _print_message_result_as_csv(result: MessageResult):
    """Print a MessageResult as CSV with a single 'message' column.

    Creates a simple CSV structure with one column named 'message'
    containing the sanitized message text.
    """
    writer = csv.DictWriter(sys.stdout, fieldnames=["message"], lineterminator="\n")
    writer.writeheader()
    writer.writerow({"message": sanitize_for_terminal(result.message)})


def _print_csv_result_streaming(result: CommandResult):
    """Print a single CommandResult as CSV with streaming support"""
    if isinstance(result, CollectionResult):
        _stream_collection_as_csv(result)
    elif isinstance(result, ObjectResult):
        _print_object_result_as_csv(result)
    elif isinstance(result, MessageResult):
        _print_message_result_as_csv(result)


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
            sanitize_for_terminal(str(key)), sanitize_for_terminal(__to_str(value))
        )
    rich_print(table, flush=True)


def print_result(cmd_result: CommandResult, output_format: OutputFormat | None = None):
    output_format = output_format or _get_format_type()
    if is_structured_format(output_format):
        print_structured(cmd_result, output_format)
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
