import json
from datetime import datetime
from textwrap import dedent
from typing import NamedTuple
from unittest.mock import Mock, patch

import pytest
from click import Context, Command
from snowflake.connector.cursor import SnowflakeCursor

from snowcli.output.formats import OutputFormat
from snowcli.output.printing import print_output, OutputData, print_db_cursor


class MockResultMetadata(NamedTuple):
    name: str


def test_print_db_cursor_table(capsys):
    print_output(OutputData(OutputFormat.TABLE).add_data(_create_mock_cursor()))

    assert _get_output(capsys) == dedent(
        """\
    +---------------------------------------------------------------------+
    | string | number | array     | object          | date                |
    |--------+--------+-----------+-----------------+---------------------|
    | string | 42     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | string | 43     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------------------------+
    """
    )


def test_print_multi_cursors_table(capsys):
    mock_cursor = _create_mock_cursor()

    output_data = (
        OutputData(OutputFormat.TABLE).add_data(mock_cursor).add_data(mock_cursor)
    )

    print_output(output_data)

    assert _get_output(capsys) == dedent(
        """\
    +---------------------------------------------------------------------+
    | string | number | array     | object          | date                |
    |--------+--------+-----------+-----------------+---------------------|
    | string | 42     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | string | 43     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------------------------+
    +---------------------------------------------------------------------+
    | string | number | array     | object          | date                |
    |--------+--------+-----------+-----------------+---------------------|
    | string | 42     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | string | 43     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------------------------+
    """
    )


def test_print_different_data_sources_table(capsys):
    output_data = (
        OutputData(OutputFormat.TABLE)
        .add_data(_create_mock_cursor())
        .add_data("Command done")
        .add_data([{"key": "value"}])
    )

    print_output(output_data)

    assert _get_output(capsys) == dedent(
        """\
    +---------------------------------------------------------------------+
    | string | number | array     | object          | date                |
    |--------+--------+-----------+-----------------+---------------------|
    | string | 42     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | string | 43     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------------------------+
    Command done
    +-------+
    | key   |
    |-------|
    | value |
    +-------+
    """
    )


def test_print_db_cursor_json(capsys):
    print_output(OutputData(OutputFormat.JSON).add_data(_create_mock_cursor()))

    assert _get_output_as_json(capsys) == [
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


def test_print_multi_db_cursor_json(capsys):
    mock_cursor = _create_mock_cursor()

    output_data = (
        OutputData(OutputFormat.JSON).add_data(mock_cursor).add_data(mock_cursor)
    )
    print_output(output_data)

    assert _get_output_as_json(capsys) == [
        [
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
        ],
        [
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
        ],
    ]


def test_print_different_data_sources_json(capsys):
    output_data = (
        OutputData(OutputFormat.JSON)
        .add_data(_create_mock_cursor())
        .add_data("Command done")
        .add_data([{"key": "value"}])
    )

    print_output(output_data)

    assert _get_output_as_json(capsys) == [
        [
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
        ],
        {"result": "Command done"},
        [{"key": "value"}],
    ]


def test_print_with_no_data_table(capsys):
    output_data = OutputData(OutputFormat.TABLE)

    print_output(output_data)

    assert _get_output(capsys) == "Done\n"


def test_print_with_no_data_json(capsys):
    output_data = OutputData(OutputFormat.JSON)

    print_output(output_data)

    assert _get_output(capsys) == "Done\n"


@patch("snowcli.output.printing.click.get_current_context")
def test_print_db_cursor_filters_columns(mock_context, capsys):
    _mock_output_format(mock_context, "JSON")

    print_db_cursor(_create_mock_cursor(), columns=["string", "object"])

    assert _get_output_as_json(capsys) == [
        {"string": "string", "object": {"k": "object"}},
        {"string": "string", "object": {"k": "object"}},
    ]


def test_raise_error_when_add_not_supported_data_type():
    with pytest.raises(TypeError) as exception:
        OutputData().add_data(None)

    assert exception.value.args[0] == "unsupported data type <class 'NoneType'>"


def _mock_output_format(mock_context, format):
    context = Context(Command("foo"))
    context.params = {"output_format": format}
    mock_context.return_value = context


def _get_output(capsys):
    captured = capsys.readouterr()
    return captured.out


def _get_output_as_json(capsys):
    return json.loads(_get_output(capsys))


def _create_mock_cursor() -> SnowflakeCursor:
    mock_cursor = Mock(spec=SnowflakeCursor)
    mock_cursor.description = [
        MockResultMetadata("string"),
        MockResultMetadata("number"),
        MockResultMetadata("array"),
        MockResultMetadata("object"),
        MockResultMetadata("date"),
    ]
    mock_cursor.fetchall.return_value = [
        ("string", 42, ["array"], {"k": "object"}, datetime(2022, 3, 21)),
        ("string", 43, ["array"], {"k": "object"}, datetime(2022, 3, 21)),
    ]
    return mock_cursor
