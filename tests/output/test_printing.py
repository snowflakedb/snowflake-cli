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

import io
import json
import logging
import re
import sys
from datetime import datetime, time
from decimal import Decimal
from textwrap import dedent
from typing import NamedTuple

import pytest
from rich.cells import cell_len
from rich.console import Console
from snowflake.cli._app import ascii_table
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


def test_query_result_duplicate_column_names_are_uniquified(mock_cursor):
    # Simulates: SELECT * FROM A LEFT JOIN B ON a.b_id = b.id
    # where both A and B have columns named "id" and "addr1"
    cursor = mock_cursor(
        columns=["id", "addr1", "b_id", "id", "addr1"],
        rows=[(1, "a_addr", 10, 99, "b_addr")],
    )
    result = QueryResult(cursor)
    assert result.column_names == ["id", "addr1", "b_id", "id_2", "addr1_2"]
    rows = list(result.result)
    assert rows == [
        {"id": 1, "addr1": "a_addr", "b_id": 10, "id_2": 99, "addr1_2": "b_addr"}
    ]


def test_query_result_uniquify_column_names_triple_duplicate(mock_cursor):
    cursor = mock_cursor(
        columns=["x", "x", "x"],
        rows=[(1, 2, 3)],
    )
    result = QueryResult(cursor)
    assert result.column_names == ["x", "x_2", "x_3"]
    rows = list(result.result)
    assert rows == [{"x": 1, "x_2": 2, "x_3": 3}]


def test_query_result_uniquify_column_names_collision_with_existing(mock_cursor):
    # "x_2" already exists as a real column — the deduplicator must skip to "_3"
    cursor = mock_cursor(
        columns=["x", "x_2", "x"],
        rows=[(1, 2, 3)],
    )
    result = QueryResult(cursor)
    assert result.column_names == ["x", "x_2", "x_3"]
    rows = list(result.result)
    assert rows == [{"x": 1, "x_2": 2, "x_3": 3}]


def test_query_result_uniquify_does_not_rename_real_column_matching_generated_suffix(
    mock_cursor,
):
    # Real column "id_2" from table B must keep its name;
    # the duplicate "id" must skip to "id_3" to avoid the clash.
    cursor = mock_cursor(
        columns=["id", "name", "id", "name", "id_2"],
        rows=[(1, "alice", 1, "bob", 2)],
    )
    result = QueryResult(cursor)
    assert result.column_names == ["id", "name", "id_3", "name_2", "id_2"]
    rows = list(result.result)
    assert rows == [{"id": 1, "name": "alice", "id_3": 1, "name_2": "bob", "id_2": 2}]


def test_query_result_no_duplicates_unchanged(mock_cursor):
    cursor = mock_cursor(
        columns=["a", "b", "c"],
        rows=[(1, 2, 3)],
    )
    result = QueryResult(cursor)
    assert result.column_names == ["a", "b", "c"]


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


def test_print_many_columns_table_is_legible(capsys, monkeypatch):
    # Regression: with many columns and a non-TTY stdout, rich's default width
    # of 80 used to squash each cell to 0-1 characters, producing a series of
    # "|" separators with no content. See GH#2725 / SNOW-2917443.
    #
    # Pin the dumb-terminal env that actually ignores a lone Console(width=…):
    # FORCE_COLOR (even "0") makes Rich report is_terminal, TERM in
    # {dumb, unknown} then makes size 80×25. GitHub Actions is TERM=unknown
    # with no FORCE_COLOR and would not catch a revert of height=.
    monkeypatch.setenv("TERM", "unknown")
    monkeypatch.setenv("FORCE_COLOR", "0")
    ncols = 30
    row = {f"col_{i}": f"value_{i}" for i in range(ncols)}
    collection = CollectionResult([row])

    print_result(collection, output_format=OutputFormat.TABLE)
    output = get_output(capsys)

    # Each column's header/data text must actually appear in the rendered table.
    for i in range(ncols):
        assert f"col_{i}" in output, f"Missing column header col_{i}"
        assert f"value_{i}" in output, f"Missing cell value value_{i}"


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


_CSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def _strip_csi(text: str) -> str:
    return _CSI.sub("", text)


@pytest.fixture(autouse=True)
def _reset_large_table_hint():
    ascii_table.reset_large_result_hint()
    yield
    ascii_table.reset_large_result_hint()


def _spy_live(monkeypatch):
    from snowflake.cli._app import printing

    entered = {"n": 0}
    real = printing.Live

    class _Spy(real):  # type: ignore[misc, valid-type]
        def __enter__(self):
            entered["n"] += 1
            return super().__enter__()

    monkeypatch.setattr(printing, "Live", _Spy)
    return entered


def _force_tty(monkeypatch, width: int, *, color: bool):
    from snowflake.cli._app import printing

    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    monkeypatch.setattr(printing, "_stdout_is_terminal", lambda: True)

    def _console():
        return Console(
            width=width,
            height=25,
            soft_wrap=True,
            markup=False,
            force_terminal=False,
            legacy_windows=False,
            color_system="standard" if color else None,
            no_color=not color,
        )

    monkeypatch.setattr(printing, "_render_console_for_table", _console)


def _print_rows(rows):
    print_result(CollectionResult(rows), output_format=OutputFormat.TABLE)


def test_small_table_matches_existing_snapshot(capsys):
    _print_rows([{"ID": "1", "NAME": "alice"}])
    assert get_output(capsys) == (
        "+------------+\n"
        "| ID | NAME  |\n"
        "|----+-------|\n"
        "| 1  | alice |\n"
        "+------------+\n"
    )


def test_normalize_ansi_bel_cr_tab_newline():
    assert ascii_table.normalize_cell("\x1b[31mred\x1b[0m") == "red"
    assert ascii_table.normalize_cell("a\ab") == "ab"
    assert ascii_table.normalize_cell("a\rb") == "ab"
    assert ascii_table.normalize_cell("a\bb") == "ab"
    assert ascii_table.normalize_cell("a\x0bb") == "ab"
    assert ascii_table.normalize_cell("a\x0cb") == "ab"
    assert ascii_table.normalize_cell("\tok") == "        ok"
    assert ascii_table.normalize_cell("a\nb") == "a\nb"


def test_normalize_bytearray_none_int_datetime():
    assert ascii_table.normalize_cell(bytearray(b"AB")) == "4142"
    assert ascii_table.normalize_cell(None) == "None"
    assert ascii_table.normalize_cell(1) == "1"
    assert ascii_table.normalize_cell(datetime(2026, 10, 6)) == "2026-10-06 00:00:00"


def test_rich_path_strips_controls(capsys):
    _print_rows([{"c": "\x1b[31mred\x1b[0m\a\r\tok"}])
    out = get_output(capsys)
    assert "\x1b" not in out
    assert "\a" not in out
    assert "red     ok" in out


def test_equivalence_no_color_both_paths(capsys, monkeypatch):
    rows = [{"ID": "1", "NAME": "alice"}, {"ID": "2", "NAME": "bob"}]
    _print_rows(rows)
    rich = get_output(capsys)
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    _print_rows(rows)
    streamed = get_output(capsys)
    assert rich == streamed
    assert "\x1b" not in streamed


def test_equivalence_color_both_paths_strip_sgr(capsys, monkeypatch):
    from rich import box
    from rich.table import Table

    rows = [{"ID": "1", "NAME": "alice"}, {"ID": "2", "NAME": "bob"}]
    _force_tty(monkeypatch, 120, color=True)
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    _print_rows(rows)
    streamed = capsys.readouterr().out
    assert "\x1b[1m" in streamed

    buffer = io.StringIO()
    console = Console(
        file=buffer,
        width=120,
        height=25,
        soft_wrap=True,
        markup=False,
        force_terminal=False,
        color_system="standard",
        no_color=False,
    )
    table = Table(show_header=True, box=box.ASCII)
    table.add_column("ID", overflow="fold")
    table.add_column("NAME", overflow="fold")
    for row in rows:
        table.add_row(
            ascii_table.normalize_cell(row["ID"]),
            ascii_table.normalize_cell(row["NAME"]),
        )
    console.print(table)
    assert _strip_csi(streamed) == _strip_csi(buffer.getvalue())

    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    _force_tty(monkeypatch, 120, color=False)
    _print_rows(rows)
    plain = capsys.readouterr().out
    assert "\x1b" not in plain


def test_probe_cells_below_exact_above(capsys, monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 4)
    entered = _spy_live(monkeypatch)

    _print_rows([{"a": "a", "b": "r1"}])
    assert entered["n"] == 1
    assert "r1" in get_output(capsys)

    entered["n"] = 0
    _print_rows([{"a": "a", "b": "r1"}, {"a": "a", "b": "r2"}])
    assert entered["n"] == 1
    assert "r2" in get_output(capsys)

    entered["n"] = 0
    _print_rows(
        [
            {"a": "a", "b": "r1"},
            {"a": "a", "b": "r2"},
            {"a": "a", "b": "r3"},
        ]
    )
    out = get_output(capsys)
    assert entered["n"] == 0
    assert "r1" in out and "r2" in out and "r3" in out


def test_probe_chars_below_exact_above(capsys, monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CHARS", 4)
    entered = _spy_live(monkeypatch)

    _print_rows([{"c": "abc"}])
    assert entered["n"] == 1
    get_output(capsys)

    entered["n"] = 0
    _print_rows([{"c": "abcd"}])
    assert entered["n"] == 1
    get_output(capsys)

    entered["n"] = 0
    _print_rows([{"c": "abcd"}, {"c": "e"}])
    out = get_output(capsys)
    assert entered["n"] == 0
    assert "abcd" in out and "| e" in out

    entered["n"] = 0
    _print_rows([{"c": "abcde"}])
    out = get_output(capsys)
    assert entered["n"] == 0
    assert any(line.count("abcde") == 1 for line in out.splitlines())


def test_probe_constants():
    assert ascii_table.PROBE_MAX_CELLS == 20_000
    assert ascii_table.PROBE_MAX_CHARS == 2 * 1024 * 1024
    assert ascii_table.PIPE_MAX_COLUMN_WIDTH == 256


def test_generator_consumed_once_not_materialized(monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 2)
    monkeypatch.setattr(ascii_table, "WRITE_CHUNK_CHARS", 1)
    events: list[str] = []

    class _Rec(io.StringIO):
        def write(self, s):
            if s:
                events.append("write")
            return super().write(s)

        def isatty(self):
            return False

    def source():
        for index in range(6):
            events.append(f"yield{index}")
            yield {"a": "x"}

    generated = source()
    monkeypatch.setattr(sys, "stdout", _Rec())
    from snowflake.cli._app.printing import _print_multiple_table_results

    _print_multiple_table_results(CollectionResult(generated))
    assert list(generated) == []
    assert [event for event in events if event.startswith("yield")] == [
        f"yield{index}" for index in range(6)
    ]
    first_write = events.index("write")
    last_yield = max(
        index for index, event in enumerate(events) if event.startswith("yield")
    )
    assert first_write < last_yield


def test_unicode_cjk_emoji_combining_border(capsys, monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    mark = "e\u0301"
    _print_rows([{"cjk": "你", "emoji": "👍", "mark": mark}])
    lines = [line for line in get_output(capsys).splitlines() if line]
    assert len({cell_len(line) for line in lines}) == 1
    content = [line for line in lines if line.startswith("|")]
    assert content
    assert all(line.endswith("|") for line in content)
    assert cell_len("你") == 2
    assert cell_len("👍") == 2
    assert cell_len(mark) == 1
    body = "\n".join(lines)
    assert "你" in body
    assert "👍" in body
    assert mark in body


def test_tty_widths_fit(capsys, monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    _force_tty(monkeypatch, 80, color=False)
    _print_rows([{"h": "abcdefghij"}])
    out = get_output(capsys)
    assert "| abcdefghij |" in out
    assert out.count("abcdefghij") == 1


def test_tty_widths_waterfill(capsys, monkeypatch):
    assert ascii_table.allocate_tty_widths([20, 10, 10], 40) == [10, 10, 10]
    assert ascii_table.allocate_tty_widths([20, 5, 5], 20) == [8, 5, 5]
    assert ascii_table.allocate_tty_widths([10, 10], 24) == [8, 9]
    assert ascii_table.allocate_tty_widths([20, 20], 10) == [8, 8]

    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    _force_tty(monkeypatch, 40, color=False)
    wide = "A" * 20
    _print_rows([{"a": wide, "b": "B" * 10, "c": "C" * 10}])
    out = get_output(capsys)
    assert "|------------+------------+------------|" in out
    assert out.count("A") == 20
    assert "A" * 10 in out


def test_tty_widths_floor_overflow(capsys, monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    _force_tty(monkeypatch, 200, color=False)
    row = {f"{index:03d}": f"{index:03d}" for index in range(100)}
    _print_rows([row])
    out = get_output(capsys)
    rule = next(
        line
        for line in out.splitlines()
        if line.startswith("|") and set(line) <= set("|+-")
    )
    parts = rule[1:-1].split("+")
    assert len(parts) == 100
    assert all(len(part) == 5 for part in parts)
    for index in range(100):
        assert f"{index:03d}" in out


def test_late_wide_value_tty_folds(capsys, monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    _force_tty(monkeypatch, 80, color=False)
    _print_rows([{"c": "abcd"}, {"c": "abcdefghij"}])
    out = get_output(capsys)
    assert "| abcd |" in out
    assert "| efgh |" in out
    assert "| ij   |" in out
    assert "abcdefghij" not in out


def test_late_wide_value_pipe_overflow_no_wrap(capsys, monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    _print_rows([{"c": "abcd"}, {"c": "abcdefghij"}, {"c": "z"}])
    out = get_output(capsys)
    assert "| abcdefghij |" in out
    assert "| z    |" in out
    assert "| efgh |" not in out


def test_one_mib_probe_value_pipe_and_tty(capsys, monkeypatch):
    million = "a" * (1024 * 1024)
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    _print_rows([{"v": million}, {"v": "z"}])
    pipe = get_output(capsys)
    long_lines = [line for line in pipe.splitlines() if "aaa" in line]
    assert len(long_lines) == 1
    assert long_lines[0].count("a") == 1024 * 1024
    assert "| " + "z".ljust(256) + " |" in pipe

    _force_tty(monkeypatch, 10, color=False)
    _print_rows([{"v": million}, {"v": "z"}])
    tty = get_output(capsys)
    data = [line for line in tty.splitlines() if "a" in line]
    assert tty.count("a") == 1024 * 1024
    assert data
    assert all("a" * 9 not in line for line in data)


def test_mid_stream_exception_has_no_closing_rule(capsys, monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)

    def rows():
        yield {"a": "1"}
        yield {"a": "2"}
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        _print_rows(rows())
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line]
    assert lines[0].startswith("+")
    assert set(lines[0]) <= set("+-")
    assert lines[-1] != lines[0]
    assert "| 1 |" in out
    assert "| 2 |" in out


def test_keyboard_interrupt_has_no_closing_rule(capsys, monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)

    def rows():
        yield {"a": "1"}
        yield {"a": "2"}
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        _print_rows(rows())
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line]
    assert lines
    assert lines[-1] != lines[0]
    assert "| 1 |" in out
    assert "| 2 |" in out


def test_broken_pipe_propagates(monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)

    class _Boom(io.StringIO):
        def write(self, s):
            raise BrokenPipeError

        def flush(self):
            return None

        def isatty(self):
            return False

    monkeypatch.setattr(sys, "stdout", _Boom())
    from snowflake.cli._app.printing import _print_multiple_table_results

    with pytest.raises(BrokenPipeError):
        _print_multiple_table_results(CollectionResult([{"a": "1"}, {"a": "2"}]))


def test_multiple_results_probe_independently(capsys, monkeypatch):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    entered = _spy_live(monkeypatch)
    print_result(
        MultipleResults(
            [
                CollectionResult([{"a": "only"}]),
                CollectionResult([{"b": "1"}, {"b": "2"}]),
            ]
        ),
        output_format=OutputFormat.TABLE,
    )
    out = get_output(capsys)
    assert entered["n"] == 1
    assert "only" in out
    assert "| 1 |" in out and "| 2 |" in out
    assert out.count("+--") >= 2


def test_stderr_hint_tty_only_once(capsys, monkeypatch, caplog):
    monkeypatch.setattr(ascii_table, "PROBE_MAX_CELLS", 1)
    caplog.set_level(logging.DEBUG, logger="snowflake.cli._app.ascii_table")

    def _run():
        _print_rows([{"a": "1"}, {"a": "2"}])

    _run()
    piped = capsys.readouterr()
    assert ascii_table.LARGE_RESULT_HINT not in piped.err
    assert "cells=" in caplog.text

    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    _run()
    _run()
    tty = capsys.readouterr()
    assert tty.err.count(ascii_table.LARGE_RESULT_HINT) == 1
