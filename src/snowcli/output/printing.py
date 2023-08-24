from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from json import JSONEncoder
from pathlib import Path
from rich import box, print, print_json
from snowflake.connector.cursor import SnowflakeCursor
from typing import List, Optional, Dict, Union

from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager
from snowcli.exception import OutputDataTypeError
from snowcli.output.formats import OutputFormat


class CustomJSONEncoder(JSONEncoder):
    """Custom JSON encoder handling serialization of non-standard types"""

    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, Path):
            return str(o)
        return super().default(o)


class OutputData:
    def __init__(
        self,
        output: Optional[Union[SnowflakeCursor, str, List[Dict]]] = None,
        format: Optional[OutputFormat] = None,
    ) -> None:
        if output is not None:
            self.output = [output]
            self.counter = 1
        else:
            self.output = []
            self.counter = 0
        self._format = format

    @staticmethod
    def from_cursor(
        cursor: SnowflakeCursor, format: Optional[OutputFormat] = None
    ) -> OutputData:
        check_if_is_cursor(cursor)
        return OutputData(cursor, format)

    @staticmethod
    def from_string(message: str, format: Optional[OutputFormat] = None) -> OutputData:
        check_if_is_string(message)
        return OutputData(message, format)

    @staticmethod
    def from_list(data: List[Dict], format: Optional[OutputFormat] = None):
        check_if_is_list(data)
        return OutputData(data, format)

    def add_cursor(self, cursor: SnowflakeCursor) -> OutputData:
        check_if_is_cursor(cursor)
        self.output.append(cursor)
        self.counter += 1
        return self

    def add_string(self, message: str) -> OutputData:
        check_if_is_string(message)
        self.output.append(message)
        self.counter += 1
        return self

    def add_list(self, data: List[Dict]):
        check_if_is_list(data)
        self.output.append(data)
        self.counter += 1
        return self

    @property
    def format(self):
        if not self._format:
            self._format = _get_format_type()
        return self._format

    def get_data(self) -> Union[str, Dict, Iterable[List[Dict]]]:
        for output in self.output:
            if isinstance(output, SnowflakeCursor):
                yield _get_data_from_cursor(output)
            elif isinstance(output, str):
                if self.format != OutputFormat.JSON:
                    yield output
                else:
                    yield {"result": output}
            elif isinstance(output, list):
                yield output

    def size(self) -> int:
        return self.counter


def check_if_is_cursor(cursor: SnowflakeCursor) -> None:
    if not isinstance(cursor, SnowflakeCursor):
        raise OutputDataTypeError(type(cursor), SnowflakeCursor)


def check_if_is_string(message: str) -> None:
    if not isinstance(message, str):
        raise OutputDataTypeError(type(message), str)


def check_if_is_list(data: List[Dict]) -> None:
    if not isinstance(data, list) or (len(data) > 0 and not isinstance(data[0], dict)):
        raise OutputDataTypeError(type(data), List[dict])


def print_output(output_data: Optional[OutputData] = None) -> None:
    if output_data is None or output_data.size() == 0:
        print("Done")
        return

    if output_data.format == OutputFormat.TABLE:
        for data in output_data.get_data():
            _print_table(data)
    elif output_data.format == OutputFormat.JSON:
        _print_json(output_data)
    else:
        raise Exception(f"Unknown {output_data.format} format option")


def _print_json(output_data: OutputData) -> None:
    import json

    data = list(output_data.get_data())
    if len(data) == 1:
        data = data[0]
    print_json(json.dumps(data, cls=CustomJSONEncoder))


def print_db_cursor(
    cursor: SnowflakeCursor,
    columns: Optional[List[str]] = None,
) -> None:
    """
    Prints results fetched by cursor using specified format.

    :param cursor: snowflake cursor for fetching results
    :param columns: list of columns that should be included in output, if
        not provided then all columns are returned
    :return:
    """
    data = _get_data_from_cursor(cursor, columns)
    _print_formatted(data)


def _get_data_from_cursor(
    cursor: SnowflakeCursor, columns: Optional[List[str]] = None
) -> List[Dict]:
    result = cursor.fetchall()
    column_names = [col.name for col in cursor.description]
    columns_to_include = columns or column_names

    return [
        {k: v for k, v in zip(column_names, row) if k in columns_to_include}
        for row in result
    ]


def _get_format_type() -> OutputFormat:
    output_format = (
        snow_cli_global_context_manager.get_global_context_copy().output_format
    )
    if output_format:
        return output_format
    return OutputFormat.TABLE


def _print_formatted(data: List[Dict]) -> None:
    output_format = _get_format_type()
    if output_format == OutputFormat.TABLE:
        _print_table(data)
    elif output_format == OutputFormat.JSON:
        import json

        print_json(json.dumps(data, cls=CustomJSONEncoder))
    else:
        raise Exception(f"Unknown {output_format} format option")


def print_data(data: List[Dict], columns: Optional[List[str]] = None) -> None:
    if columns is not None:
        data = [{k: v for k, v in raw.items() if k in columns} for raw in data]
    _print_formatted(data)


def _print_table(data: Union[str, Dict, List[Dict]]) -> None:
    from rich.table import Table

    if not data:
        print("No data")
        return

    if isinstance(data, str):
        print(data)
        return

    columns = list(data[0].keys())

    table = Table(show_header=True, box=box.ASCII)
    for column in columns:
        table.add_column(column)
    for row in data:
        table.add_row(*[str(i) for i in row.values()])
    print(table)
