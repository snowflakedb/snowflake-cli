from __future__ import annotations

import sys
from datetime import datetime
from json import JSONEncoder
from pathlib import Path
from rich import box, print
from rich.live import Live
from rich.table import Table
from typing import Union
import json

from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager
from snowcli.output.formats import OutputFormat
from snowcli.output.types import (
    MessageResult,
    ObjectResult,
    CollectionResult,
    CommandResult,
    MultipleResults,
    QueryResult,
)

NO_ITEMS_FOUND: str = "No data"


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
    output_format = (
        snow_cli_global_context_manager.get_global_context_copy().output_format
    )
    if output_format:
        return output_format
    return OutputFormat.TABLE


def _get_table():
    return Table(show_header=True, box=box.ASCII)


def _print_multiple_table_results(obj: CollectionResult):
    if isinstance(obj, QueryResult):
        print(obj.query)
    items = obj.result
    try:
        first_item = next(items)
    except StopIteration:
        print(NO_ITEMS_FOUND)
        print()
        return
    table = _get_table()
    for column in first_item.keys():
        table.add_column(column, overflow="fold")
    with Live(table, refresh_per_second=4):
        table.add_row(*[str(i) for i in first_item.values()])
        for item in items:
            table.add_row(*[str(i) for i in item.values()])
    # Add separator between tables
    print()


def is_structured_format(output_format):
    return output_format == OutputFormat.JSON


def print_structured(result: CommandResult):
    """Handles outputs like json, yml and other structured and parsable formats."""
    import json

    return json.dump(result, sys.stdout, cls=CustomJSONEncoder, indent=4)


def print_unstructured(obj: CommandResult | None):
    """Handles outputs like table, plain text and other unstructured types."""
    if not obj:
        print("Done")
    elif not obj.result:
        print("No data")
    elif isinstance(obj, MessageResult):
        print(obj.message)
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
    print(table)


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
