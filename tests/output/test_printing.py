import json
from datetime import datetime
from textwrap import dedent

from click import Command, Context
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import (
    CollectionResult,
    MessageResult,
    MultipleResults,
    ObjectResult,
    QueryResult,
    SingleQueryResult,
)
from snowflake.cli.app.printing import print_result

from tests.testing_utils.fixtures import *


class MockResultMetadata(NamedTuple):
    name: str


def test_single_value_from_query(capsys, mock_cursor):
    output_data = SingleQueryResult(
        mock_cursor(
            columns=["array", "object", "date"],
            rows=[
                (["array"], {"k": "object"}, datetime(2022, 3, 21)),
            ],
        )
    )

    print_result(output_data, output_format=OutputFormat.TABLE)
    assert _get_output(capsys) == dedent(
        """\
    +------------------------------+
    | key    | value               |
    |--------+---------------------|
    | array  | ['array']           |
    | object | {'k': 'object'}     |
    | date   | 2022-03-21 00:00:00 |
    +------------------------------+
    """
    )


def test_single_object_result(capsys, mock_cursor):
    output_data = ObjectResult(
        {"array": ["array"], "object": {"k": "object"}, "date": datetime(2022, 3, 21)}
    )

    print_result(output_data, output_format=OutputFormat.TABLE)
    assert _get_output(capsys) == dedent(
        """\
    +------------------------------+
    | key    | value               |
    |--------+---------------------|
    | array  | ['array']           |
    | object | {'k': 'object'}     |
    | date   | 2022-03-21 00:00:00 |
    +------------------------------+
    """
    )


def test_single_collection_result(capsys, mock_cursor):
    output_data = {
        "array": ["array"],
        "object": {"k": "object"},
        "date": datetime(2022, 3, 21),
    }
    collection = CollectionResult([output_data, output_data])

    print_result(collection, output_format=OutputFormat.TABLE)
    assert _get_output(capsys) == dedent(
        """\
    +---------------------------------------------------+
    | array     | object          | date                |
    |-----------+-----------------+---------------------|
    | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------+
    """
    )


def test_print_multi_results_table(capsys, _create_mock_cursor):
    output_data = MultipleResults(
        [
            QueryResult(_create_mock_cursor()),
            QueryResult(_create_mock_cursor()),
        ],
    )

    print_result(output_data, output_format=OutputFormat.TABLE)

    assert _get_output(capsys) == dedent(
        """\
    SELECT A MOCK QUERY
    +---------------------------------------------------------------------+
    | string | number | array     | object          | date                |
    |--------+--------+-----------+-----------------+---------------------|
    | string | 42     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | string | 43     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------------------------+
    SELECT A MOCK QUERY
    +---------------------------------------------------------------------+
    | string | number | array     | object          | date                |
    |--------+--------+-----------+-----------------+---------------------|
    | string | 42     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | string | 43     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------------------------+
    """
    )


def test_print_different_multi_results_table(capsys, mock_cursor):
    output_data = MultipleResults(
        [
            QueryResult(
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
            QueryResult(
                mock_cursor(
                    columns=["array", "object", "date"],
                    rows=[
                        (["array"], {"k": "object"}, datetime(2022, 3, 21)),
                        (["array"], {"k": "object"}, datetime(2022, 3, 21)),
                    ],
                )
            ),
        ],
    )

    print_result(output_data, output_format=OutputFormat.TABLE)

    assert _get_output(capsys) == dedent(
        """\
    SELECT A MOCK QUERY
    +-----------------+
    | string | number |
    |--------+--------|
    | string | 42     |
    | string | 43     |
    +-----------------+
    SELECT A MOCK QUERY
    +---------------------------------------------------+
    | array     | object          | date                |
    |-----------+-----------------+---------------------|
    | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------+
    """
    )


def test_print_different_data_sources_table(capsys, _create_mock_cursor):
    output_data = MultipleResults(
        [
            QueryResult(_create_mock_cursor()),
            MessageResult("Command done"),
            CollectionResult(({"key": "value"} for _ in range(1))),
        ],
    )

    print_result(output_data, output_format=OutputFormat.TABLE)

    assert _get_output(capsys) == dedent(
        """\
    SELECT A MOCK QUERY
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


def test_print_multi_db_cursor_json(capsys, _create_mock_cursor):
    output_data = MultipleResults(
        [
            QueryResult(_create_mock_cursor()),
            QueryResult(_create_mock_cursor()),
        ],
    )
    print_result(output_data, output_format=OutputFormat.JSON)

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
    output_data = MultipleResults(
        [
            QueryResult(_create_mock_cursor()),
            MessageResult("Command done"),
            CollectionResult(({"key": f"value_{i}"} for i in range(2))),
        ],
    )

    print_result(output_data, output_format=OutputFormat.JSON)

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
        {"message": "Command done"},
        [{"key": "value_0"}, {"key": "value_1"}],
    ]


def test_print_with_no_data_table(capsys):
    print_result(None)
    assert _get_output(capsys) == "Done\n"


def test_print_with_no_data_in_query_json(capsys, _empty_cursor):
    print_result(QueryResult(_empty_cursor()), output_format=OutputFormat.JSON)
    assert _get_output(capsys) == "[]"


def test_print_with_no_data_in_single_value_query_json(capsys, _empty_cursor):
    print_result(SingleQueryResult(_empty_cursor()), output_format=OutputFormat.JSON)
    assert _get_output(capsys) == "null"


def test_print_with_no_response_json(capsys):
    print_result(None, output_format=OutputFormat.JSON)

    assert _get_output(capsys) == "null"


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


@pytest.fixture
def _empty_cursor(mock_cursor):
    return lambda: mock_cursor(
        columns=["string", "number", "array", "object", "date"],
        rows=[],
    )
