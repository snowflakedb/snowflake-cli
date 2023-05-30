import json
from datetime import datetime
from textwrap import dedent
from typing import NamedTuple
from unittest import mock

from click import Context, Command
from snowcli.output.printing import print_db_cursor


class MockResultMetadata(NamedTuple):
    name: str


class _MockCursor:
    def __init__(self):
        self._rows = [
            ("string", 42, ["array"], {"k": "object"}, datetime(2022, 3, 21)),
            ("string", 43, ["array"], {"k": "object"}, datetime(2022, 3, 21)),
        ]
        self._columns = [
            MockResultMetadata("string"),
            MockResultMetadata("number"),
            MockResultMetadata("array"),
            MockResultMetadata("object"),
            MockResultMetadata("date"),
        ]

    def fetchall(self):
        yield from self._rows

    @property
    def description(self):
        yield from self._columns


@mock.patch("snowcli.output.printing.click.get_current_context")
def test_print_db_cursor_table(context_mock, capsys):
    context = Context(Command("foo"))
    context.params = {"output_format": "TABLE"}
    context_mock.return_value = context

    cur = _MockCursor()
    print_db_cursor(cur)

    captured = capsys.readouterr()
    assert captured.out == dedent(
        """\
    +---------------------------------------------------------------------+
    | string | number | array     | object          | date                |
    |--------+--------+-----------+-----------------+---------------------|
    | string | 42     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | string | 43     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------------------------+
    """
    )


@mock.patch("snowcli.output.printing.click.get_current_context")
def test_print_db_cursor_json(context_mock, capsys):
    context = Context(Command("foo"))
    context.params = {"output_format": "JSON"}
    context_mock.return_value = context

    cur = _MockCursor()
    print_db_cursor(cur)

    captured = capsys.readouterr()
    json_output = json.loads(captured.out)
    assert json_output == [
        {
            "string": "string",
            "number": 42,
            "array": ["array"],
            "object": {"k": "object"},
            "date": "2022-03-21T00:00:00",
        },
        {
            "string": "string",
            "number": 43,
            "array": ["array"],
            "object": {"k": "object"},
            "date": "2022-03-21T00:00:00",
        },
    ]


@mock.patch("snowcli.output.printing.click.get_current_context")
def test_print_db_cursor_filters_columns(context_mock, capsys):
    context = Context(Command("foo"))
    context.params = {"output_format": "JSON"}
    context_mock.return_value = context

    cur = _MockCursor()
    print_db_cursor(cur, columns=["string", "object"])

    captured = capsys.readouterr()
    json_output = json.loads(captured.out)
    assert json_output == [
        {"string": "string", "object": {"k": "object"}},
        {"string": "string", "object": {"k": "object"}},
    ]
