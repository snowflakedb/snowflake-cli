import json
from datetime import datetime
from textwrap import dedent
from typing import NamedTuple
from unittest.mock import Mock

import pytest
from click import Context, Command
from snowflake.connector.cursor import SnowflakeCursor

from snowcli.exception import OutputDataTypeError
from snowcli.output.formats import OutputFormat
from snowcli.output.printing import print_output, OutputData


class MockResultMetadata(NamedTuple):
    name: str


def test_print_db_cursor_table(capsys):
    output_data = OutputData.from_cursor(_create_mock_cursor(), OutputFormat.TABLE)
    print_output(output_data)

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
        OutputData(format=OutputFormat.TABLE)
        .add_cursor(mock_cursor)
        .add_cursor(mock_cursor)
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
        OutputData(format=OutputFormat.TABLE)
        .add_cursor(_create_mock_cursor())
        .add_string("Command done")
        .add_list([{"key": "value"}])
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
    print_output(OutputData(format=OutputFormat.JSON).add_cursor(_create_mock_cursor()))

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
        OutputData(format=OutputFormat.JSON)
        .add_cursor(mock_cursor)
        .add_cursor(mock_cursor)
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
        OutputData(format=OutputFormat.JSON)
        .add_cursor(_create_mock_cursor())
        .add_string("Command done")
        .add_list([{"key": "value"}])
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
    output_data = OutputData(format=OutputFormat.TABLE)

    print_output(output_data)

    assert _get_output(capsys) == "Done\n"


def test_print_with_no_data_json(capsys):
    output_data = OutputData(format=OutputFormat.JSON)

    print_output(output_data)

    assert _get_output(capsys) == "Done\n"


def test_raise_error_when_try_add_wrong_data_type_to_from_cursor():
    with pytest.raises(OutputDataTypeError) as exception:
        OutputData.from_cursor("")

    assert (
        exception.value.args[0]
        == "Got <class 'str'> type but expected <class 'snowflake.connector.cursor.SnowflakeCursor'>"
    )


def test_raise_error_when_try_add_wrong_data_type_to_from_string():
    with pytest.raises(OutputDataTypeError) as exception:
        OutputData.from_string(0)

    assert (
        exception.value.args[0] == "Got <class 'int'> type but expected <class 'str'>"
    )


def test_raise_error_when_try_add_wrong_data_type_to_from_list():
    with pytest.raises(OutputDataTypeError) as exception:
        OutputData.from_list("")

    assert (
        exception.value.args[0]
        == "Got <class 'str'> type but expected typing.List[dict]"
    )


def test_raise_error_when_try_add_wrong_data_type_to_add_cursor():
    with pytest.raises(OutputDataTypeError) as exception:
        OutputData().add_cursor("")

    assert (
        exception.value.args[0]
        == "Got <class 'str'> type but expected <class 'snowflake.connector.cursor.SnowflakeCursor'>"
    )


def test_raise_error_when_try_add_wrong_data_type_to_add_string():
    with pytest.raises(OutputDataTypeError) as exception:
        OutputData().add_string(0)

    assert (
        exception.value.args[0] == "Got <class 'int'> type but expected <class 'str'>"
    )


def test_raise_error_when_try_add_wrong_data_type_to_add_list():
    with pytest.raises(OutputDataTypeError) as exception:
        OutputData().add_list("")

    assert (
        exception.value.args[0]
        == "Got <class 'str'> type but expected typing.List[dict]"
    )


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
