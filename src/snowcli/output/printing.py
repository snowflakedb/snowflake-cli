from __future__ import annotations

from datetime import datetime
from json import JSONEncoder
from pathlib import Path
from rich import box, print, print_json
from rich.live import Live
from rich.table import Table
from snowflake.connector.cursor import SnowflakeCursor
from typing import List, Optional, Dict, Union, Iterator

from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager
from snowcli.exception import OutputDataTypeError
from snowcli.output.formats import OutputFormat


class CustomJSONEncoder(JSONEncoder):
    """Custom JSON encoder handling serialization of non-standard types"""

    def default(self, o):
        if isinstance(o, OutputData):
            return o.as_json()
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, Path):
            return str(o)
        return super().default(o)


class OutputData:
    """
    This class constitutes base for returning output of commands. Every command wishing to return some
    information to end users should return `OutputData` and use `@with_output` decorator.

    This implementation can handle streams of outputs. This helps with automated iteration through snowflake
    cursors as well with cases when you want to stream constant output (for example logs).
    """

    def __init__(
        self,
        stream: Optional[Iterator[Union[Dict, OutputData]]] = None,
        format_: Optional[OutputFormat] = None,
    ) -> None:
        self._stream = stream
        self._format = format_

    @classmethod
    def from_cursor(
        cls, cursor: SnowflakeCursor, format_: Optional[OutputFormat] = None
    ) -> OutputData:
        """Converts Snowflake cursor to stream of data"""
        if not isinstance(cursor, SnowflakeCursor):
            raise OutputDataTypeError(type(cursor), SnowflakeCursor)
        return OutputData(stream=_get_data_from_cursor(cursor), format_=format_)

    @classmethod
    def from_string(
        cls, message: str, format_: Optional[OutputFormat] = None
    ) -> OutputData:
        """Coverts string to stream of data"""
        if not isinstance(message, str):
            raise OutputDataTypeError(type(message), str)
        return cls(stream=({"result": message} for _ in range(1)), format_=format_)

    @classmethod
    def from_list(
        cls, data: List[Union[Dict, OutputData]], format_: Optional[OutputFormat] = None
    ) -> OutputData:
        """Converts list to stream of data."""
        if not isinstance(data, list) or (
            len(data) > 0 and not isinstance(data[0], (dict, OutputData))
        ):
            raise OutputDataTypeError(type(data), List[Union[Dict, OutputData]])
        return cls(stream=(item for item in data), format_=format_)

    @property
    def format(self) -> OutputFormat:
        if not self._format:
            self._format = _get_format_type()
        return self._format

    def is_empty(self) -> bool:
        return self._stream is None

    def get_data(self) -> Iterator[Union[Dict, OutputData]]:
        """Returns iterator over output data"""
        if not self._stream:
            return None

        yield from self._stream

    def print(self):
        _print_output(self)

    def as_json(self):
        return list(self._stream)


def _print_output(output_data: Optional[OutputData] = None) -> None:
    if output_data is None:
        print("Done")
        return

    if output_data.is_empty():
        print("No data")
        return

    if output_data.format == OutputFormat.TABLE:
        _render_table_output(output_data)
    elif output_data.format == OutputFormat.JSON:
        _print_json(output_data)
    else:
        raise Exception(f"Unknown {output_data.format} format option")


def _print_json(output_data: OutputData) -> None:
    import json

    print_json(json.dumps(output_data.as_json(), cls=CustomJSONEncoder))


def _get_data_from_cursor(
    cursor: SnowflakeCursor, columns: Optional[List[str]] = None
) -> Iterator[Dict]:
    column_names = [col.name for col in cursor.description]
    columns_to_include = columns or column_names

    return (
        {k: v for k, v in zip(column_names, row) if k in columns_to_include}
        for row in cursor
    )


def _get_format_type() -> OutputFormat:
    output_format = (
        snow_cli_global_context_manager.get_global_context_copy().output_format
    )
    if output_format:
        return output_format
    return OutputFormat.TABLE


def _render_table_output(data: OutputData) -> None:
    stream = data.get_data()
    for item in stream:
        if isinstance(item, OutputData):
            _render_table_output(item)
        else:
            _print_table(item, stream)


def _print_table(item, stream):
    table = Table(show_header=True, box=box.ASCII)
    for column in item.keys():
        table.add_column(column)
    with Live(table, refresh_per_second=4):
        table.add_row(*[str(i) for i in item.values()])
        for item in stream:
            table.add_row(*[str(i) for i in item.values()])
    # Add separator between tables
    print()
