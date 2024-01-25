from __future__ import annotations

import json
import sys
from datetime import datetime
from json import JSONEncoder
from pathlib import Path

from rich import box, get_console
from rich import print as rich_print
from rich.live import Live
from rich.table import Table
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    MultipleResults,
    ObjectResult,
    QueryResult,
)

NO_ITEMS_FOUND: str = "No data"

# ensure we do not break URLs that wrap lines
get_console().soft_wrap = True


class CustomJSONEncoder(JSONEncoder):
    """Custom JSON encoder handling serialization of non-standard types"""

    def default(self, o):
        if isinstance(o, (ObjectResult, MessageResult)):
            return o.result
        if isinstance(o, (CollectionResult, MultipleResults)):
            return list(o.result)
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, Path):
            return str(o)
        return super().default(o)


def _get_format_type() -> OutputFormat:
    output_format = cli_context.output_format
    if output_format:
        return output_format
    return OutputFormat.TABLE


def _get_table():
    return Table(show_header=True, box=box.ASCII)


def _print_multiple_table_results(obj: CollectionResult):
    if isinstance(obj, QueryResult):
        rich_print(obj.query)
    items = obj.result
    try:
        first_item = next(items)
    except StopIteration:
        rich_print(NO_ITEMS_FOUND)
        rich_print()
        return
    table = _get_table()
    for column in first_item.keys():
        table.add_column(column, overflow="fold")
    with Live(table, refresh_per_second=4):
        table.add_row(*[str(i) for i in first_item.values()])
        for item in items:
            table.add_row(*[str(i) for i in item.values()])
    # Add separator between tables
    rich_print()


def is_structured_format(output_format):
    return output_format == OutputFormat.JSON


def print_structured(result: CommandResult):
    """Handles outputs like json, yml and other structured and parsable formats."""
    if isinstance(result, MultipleResults):
        _stream_json(result)
    else:
        return json.dump(result, sys.stdout, cls=CustomJSONEncoder, indent=4)


def _stream_json(result):
    """Simple helper for streaming multiple results as a JSON."""
    print("[")
    results = result.result
    res = next(results, None)
    while res:
        json.dump(res, sys.stdout, cls=CustomJSONEncoder, indent=4)
        if res := next(results, None):
            print(",")
    print("\n]")


def print_unstructured(obj: CommandResult | None):
    """Handles outputs like table, plain text and other unstructured types."""
    if not obj:
        rich_print("Done")
    elif not obj.result:
        rich_print("No data")
    elif isinstance(obj, MessageResult):
        rich_print(obj.message)
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
        table.add_row(str(key), str(value))
    rich_print(table)


def print_result(cmd_result: CommandResult, output_format: OutputFormat | None = None):
    output_format = output_format or _get_format_type()
    if is_structured_format(output_format):
        print_structured(cmd_result)
    elif isinstance(cmd_result, MultipleResults):
        for res in cmd_result.result:
            print_result(res)
    elif (
        isinstance(cmd_result, (MessageResult, ObjectResult, CollectionResult))
        or cmd_result is None
    ):
        print_unstructured(cmd_result)
    else:
        raise ValueError(f"Unexpected type {type(cmd_result)}")
