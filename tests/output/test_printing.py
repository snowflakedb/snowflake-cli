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

import json
from datetime import datetime, time
from decimal import Decimal
from textwrap import dedent
from typing import NamedTuple

import pytest
from snowflake.cli._app.printing import print_result
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import (
    CollectionResult,
    MessageResult,
    MultipleResults,
    ObjectResult,
    QueryResult,
    SingleQueryResult,
    StreamResult,
)

from tests.testing_utils.conversion import get_output, get_output_as_json


class MockResultMetadata(NamedTuple):
    name: str
    type_code: int = 1  # Default to a non-JSON type for most tests


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
    assert get_output(capsys) == dedent(
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

    print_result(output_data, output_format=OutputFormat.CSV)
    assert get_output(capsys) == dedent(
        """\
array,object,date
['array'],{'k': 'object'},2022-03-21T00:00:00
    """
    )


def test_single_object_result(capsys, mock_cursor):
    output_data = ObjectResult(
        {"array": ["array"], "object": {"k": "object"}, "date": datetime(2022, 3, 21)}
    )

    print_result(output_data, output_format=OutputFormat.TABLE)
    assert get_output(capsys) == dedent(
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

    print_result(output_data, output_format=OutputFormat.CSV)
    assert get_output(capsys) == dedent(
        """\
array,object,date
['array'],{'k': 'object'},2022-03-21T00:00:00
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
    assert get_output(capsys) == dedent(
        """\
    +---------------------------------------------------+
    | array     | object          | date                |
    |-----------+-----------------+---------------------|
    | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------+
    """
    )

    print_result(collection, output_format=OutputFormat.CSV)
    assert get_output(capsys) == dedent(
        """\
array,object,date
['array'],{'k': 'object'},2022-03-21T00:00:00
['array'],{'k': 'object'},2022-03-21T00:00:00
    """
    )


def test_print_markup_tags_in_output_do_not_raise_errors(capsys, mock_cursor):
    output_data = QueryResult(
        mock_cursor(
            columns=["CONCAT('[INST]','FOO', 'TRANSCRIPT','[/INST]')"],
            rows=[
                ("[INST]footranscript[/INST]",),
            ],
        )
    )
    print_result(output_data, output_format=OutputFormat.TABLE)

    assert get_output(capsys) == dedent(
        """\
    +------------------------------------------------+
    | CONCAT('[INST]','FOO', 'TRANSCRIPT','[/INST]') |
    |------------------------------------------------|
    | [INST]footranscript[/INST]                     |
    +------------------------------------------------+
    """
    )


def test_print_multi_results_table(capsys, _multiple_results):
    print_result(_multiple_results, output_format=OutputFormat.TABLE)

    assert get_output(capsys) == dedent(
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


def test_print_multi_results_csv(capsys, _multiple_results):
    print_result(_multiple_results, output_format=OutputFormat.CSV)

    assert get_output(capsys) == dedent(
        """\
string,number,array,object,date
string,42,['array'],{'k': 'object'},2022-03-21T00:00:00
string,43,['array'],{'k': 'object'},2022-03-21T00:00:00

string,number,array,object,date
string,42,['array'],{'k': 'object'},2022-03-21T00:00:00
string,43,['array'],{'k': 'object'},2022-03-21T00:00:00

    """
    )


def test_print_different_multi_results_table(capsys, _multiple_different_results):
    print_result(_multiple_different_results, output_format=OutputFormat.TABLE)

    assert get_output(capsys) == dedent(
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


def test_print_different_multi_results_csv(capsys, _multiple_different_results):
    print_result(_multiple_different_results, output_format=OutputFormat.CSV)

    assert get_output(capsys) == dedent(
        """\
string,number
string,42
string,43

array,object,date
['array'],{'k': 'object'},2022-03-21T00:00:00
['array'],{'k': 'object'},2022-03-21T00:00:00

    """
    )


def test_print_different_data_sources_table(capsys, _multiple_data_sources):
    print_result(_multiple_data_sources, output_format=OutputFormat.TABLE)

    assert get_output(capsys) == dedent(
        """\
    +---------------------------------------------------------------------+
    | string | number | array     | object          | date                |
    |--------+--------+-----------+-----------------+---------------------|
    | string | 42     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    | string | 43     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
    +---------------------------------------------------------------------+
    Command done
    +---------+
    | key     |
    |---------|
    | value_0 |
    | value_1 |
    +---------+
    """
    )


def test_print_different_data_sources_csv(capsys, _multiple_data_sources):
    print_result(_multiple_data_sources, output_format=OutputFormat.CSV)

    assert get_output(capsys) == dedent(
        """\
string,number,array,object,date
string,42,['array'],{'k': 'object'},2022-03-21T00:00:00
string,43,['array'],{'k': 'object'},2022-03-21T00:00:00

message
Command done

key
value_0
value_1

    """
    )


def test_print_multi_db_cursor_json(capsys, _multiple_results):
    print_result(_multiple_results, output_format=OutputFormat.JSON)

    assert get_output_as_json(capsys) == [
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


def test_print_different_data_sources_json(capsys, _multiple_data_sources):
    print_result(_multiple_data_sources, output_format=OutputFormat.JSON)

    assert get_output_as_json(capsys) == [
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
    assert get_output(capsys) == "Done\n"


def test_print_with_no_data_in_query_json(capsys, _empty_cursor):
    print_result(QueryResult(_empty_cursor()), output_format=OutputFormat.JSON)
    json_str = get_output(capsys)
    json.loads(json_str)
    assert json_str == "[]\n"


def test_print_with_no_data_in_single_value_query_json(capsys, _empty_cursor):
    print_result(SingleQueryResult(_empty_cursor()), output_format=OutputFormat.JSON)
    json_str = get_output(capsys)
    json.loads(json_str)
    assert json_str == "null\n"


def test_print_with_no_response_json(capsys):
    print_result(None, output_format=OutputFormat.JSON)

    json_str = get_output(capsys)
    json.loads(json_str)
    assert json_str == "null\n"


def test_print_decimal(capsys, mock_cursor):
    query_result = QueryResult(
        mock_cursor(
            columns=["decimal"],
            rows=[(Decimal("123.4567"),)],
        )
    )
    print_result(query_result, output_format=OutputFormat.JSON)

    assert get_output_as_json(capsys) == [
        {
            "decimal": "123.4567",
        }
    ]


def test_print_time(capsys, mock_cursor):
    query_result = QueryResult(
        mock_cursor(
            columns=["time"],
            rows=[(time(1, 23, 45),)],
        )
    )
    print_result(query_result, output_format=OutputFormat.JSON)

    assert get_output_as_json(capsys) == [
        {
            "time": "01:23:45",
        }
    ]


def test_print_bytearray(capsys, _bytearray_result):
    print_result(_bytearray_result, output_format=OutputFormat.TABLE)

    assert get_output(capsys) == dedent(
        """\
    +----------------------------------+
    | BYTE_ARRAY                       |
    |----------------------------------|
    | 544849532053484f554c4420574f524b |
    +----------------------------------+
    """
    )


def test_print_bytearray_json(capsys, _bytearray_result):
    print_result(_bytearray_result, output_format=OutputFormat.JSON)

    assert get_output_as_json(capsys) == [
        {
            "BYTE_ARRAY": "544849532053484f554c4420574f524b",
        }
    ]


def test_print_stream_result(capsys, _stream):
    print_result(_stream)
    assert get_output(capsys) == dedent(
        """\
        1
        +-------------+
        | key | value |
        |-----+-------|
        | 2   | 3     |
        +-------------+
        """
    )


def test_print_stream_result_json(capsys, _stream):
    print_result(_stream, output_format=OutputFormat.JSON)
    output = get_output(capsys)
    lines = output.splitlines()
    assert [json.loads(line) for line in lines if line] == [
        {"message": "1"},
        {"2": "3"},
    ]


def test_print_stream_result_csv(capsys, _stream):
    print_result(_stream, output_format=OutputFormat.CSV)
    assert get_output(capsys) == dedent(
        """\
message
1

2
3

        """
    )


@pytest.fixture
def _empty_cursor(mock_cursor):
    return lambda: mock_cursor(
        columns=["string", "number", "array", "object", "date"],
        rows=[],
    )


@pytest.fixture
def _multiple_results(_create_mock_cursor):
    return MultipleResults(
        [
            QueryResult(_create_mock_cursor()),
            QueryResult(_create_mock_cursor()),
        ],
    )


@pytest.fixture
def _multiple_different_results(mock_cursor):
    return MultipleResults(
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


@pytest.fixture
def _multiple_data_sources(_create_mock_cursor):
    return MultipleResults(
        [
            QueryResult(_create_mock_cursor()),
            MessageResult("Command done"),
            CollectionResult(({"key": f"value_{_}"} for _ in range(2))),
        ],
    )


@pytest.fixture
def _stream():
    def g():
        yield MessageResult("1")
        yield ObjectResult({"2": "3"})

    return StreamResult(g())


@pytest.fixture
def _bytearray_result(mock_cursor):
    # expected hex value: 544849532053484f554c4420574f524b
    return QueryResult(
        mock_cursor(
            columns=["BYTE_ARRAY"],
            rows=[(bytearray("THIS SHOULD WORK", "utf-8"),)],
        )
    )
