import json
from datetime import datetime
from textwrap import dedent

from click import Context, Command

from snowcli.exception import OutputDataTypeError
from snowcli.output.formats import OutputFormat
from snowcli.output.printing import OutputData

from tests.testing_utils.fixtures import *


class MockResultMetadata(NamedTuple):
    name: str


def test_print_multi_cursors_table(capsys, _create_mock_cursor):
    output_data = OutputData.from_list(
        [
            OutputData.from_cursor(_create_mock_cursor()),
            OutputData.from_cursor(_create_mock_cursor()),
        ],
        format_=OutputFormat.TABLE,
    )

    output_data.print()

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


def test_print_different_multi_cursors_table(capsys, mock_cursor):
    output_data = OutputData.from_list(
        [
            OutputData.from_cursor(
                mock_cursor(
                    columns=["string", "number"],
                    rows=[
                        (
                            "string",
                            42,
                            ["array"],
                            {"k": "object"},
                            datetime(2022, 3, 21),
                        ),
                        (
                            "string",
                            43,
                            ["array"],
                            {"k": "object"},
                            datetime(2022, 3, 21),
                        ),
                    ],
                )
            ),
            OutputData.from_cursor(
                mock_cursor(
                    columns=["array", "object", "date"],
                    rows=[
                        (["array"], {"k": "object"}, datetime(2022, 3, 21)),
                        (["array"], {"k": "object"}, datetime(2022, 3, 21)),
                    ],
                )
            ),
        ],
        format_=OutputFormat.TABLE,
    )

    output_data.print()

    assert _get_output(capsys) == dedent(
        """\
    +-----------------+
    | string | number |
    |--------+--------|
    | string | 42     |
    | string | 43     |
    +-----------------+
    +---------------------------------------------------+
    | array     | object          | date                |
    |-----------+-----------------+---------------------|
    | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------+
    """
    )


def test_print_different_data_sources_table(capsys, _create_mock_cursor):
    output_data = OutputData.from_list(
        [
            OutputData.from_cursor(_create_mock_cursor()),
            OutputData.from_string("Command done"),
            OutputData.from_list([{"key": "value"}]),
        ],
        format_=OutputFormat.TABLE,
    )

    output_data.print()

    assert _get_output(capsys) == dedent(
        """\
    +---------------------------------------------------------------------+
    | string | number | array     | object          | date                |
    |--------+--------+-----------+-----------------+---------------------|
    | string | 42     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | string | 43     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------------------------+
    +--------------+
    | result       |
    |--------------|
    | Command done |
    +--------------+
    +-------+
    | key   |
    |-------|
    | value |
    +-------+
    """
    )


def test_print_multi_db_cursor_json(capsys, _create_mock_cursor):
    output_data = OutputData.from_list(
        [
            OutputData.from_cursor(_create_mock_cursor()),
            OutputData.from_cursor(_create_mock_cursor()),
        ],
        format_=OutputFormat.JSON,
    )
    output_data.print()

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


def test_print_different_data_sources_json(capsys, _create_mock_cursor):
    output_data = OutputData.from_list(
        [
            OutputData.from_cursor(_create_mock_cursor()),
            OutputData.from_string("Command done"),
            OutputData.from_list([{"key": "value"}]),
        ],
        format_=OutputFormat.JSON,
    )

    output_data.print()

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
        [{"result": "Command done"}],
        [{"key": "value"}],
    ]


def test_print_with_no_data_table(capsys):
    output_data = OutputData(format_=OutputFormat.TABLE)

    output_data.print()

    assert _get_output(capsys) == "No data\n"


def test_print_with_no_data_json(capsys):
    output_data = OutputData(format_=OutputFormat.JSON)

    output_data.print()

    assert _get_output(capsys) == "No data\n"


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
        == "Got <class 'str'> type but expected typing.List[typing.Union[typing.Dict, "
        "snowcli.output.printing.OutputData]]"
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


@pytest.fixture
def _create_mock_cursor(mock_cursor):
    return lambda: mock_cursor(
        columns=["string", "number", "array", "object", "date"],
        rows=[
            ("string", 42, ["array"], {"k": "object"}, datetime(2022, 3, 21)),
            ("string", 43, ["array"], {"k": "object"}, datetime(2022, 3, 21)),
        ],
    )
