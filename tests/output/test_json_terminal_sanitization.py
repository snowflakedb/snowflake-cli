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
from datetime import datetime
from decimal import Decimal
from io import StringIO
from pathlib import Path

import pytest
from snowflake.cli._app.printing import (
    CustomJSONEncoder,
    StreamingJSONEncoder,
    print_result,
)
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import (
    CollectionResult,
    MessageResult,
    MultipleResults,
    ObjectResult,
    QueryResult,
    StreamResult,
)
from snowflake.cli.api.sanitizers import sanitize_for_terminal

from tests.testing_utils.conversion import get_output

# Sequences named in SNOW-3712352. OSC forms lose only the ESC-anchored Fe
# prefix under the current sanitizer; CSI forms are removed entirely.
_CSI_CLEAR = "\x1b[2J\x1b[1;1H"
_OSC_TITLE = "\x1b]0;PWNED\x07"
_OSC8 = "\x1b]8;;http://evil.example\x07click\x1b]8;;\x07"
_CELL = f"before{_OSC_TITLE}{_CSI_CLEAR}{_OSC8}after"


def _expected(value):
    """Value after terminal sanitization and JSON round-trip of containers."""
    if isinstance(value, str):
        return sanitize_for_terminal(value)
    if isinstance(value, dict):
        return {
            _expected(key) if isinstance(key, str) else key: _expected(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_expected(item) for item in value]
    return value


def _assert_no_raw_esc(value):
    if isinstance(value, str):
        assert "\x1b" not in value
    elif isinstance(value, dict):
        for key, item in value.items():
            _assert_no_raw_esc(key)
            _assert_no_raw_esc(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_raw_esc(item)


def _assert_sanitized_json(encoded: str, original):
    assert "\\u001b" not in encoded
    assert "\x1b" not in encoded
    decoded = json.loads(encoded)
    _assert_no_raw_esc(decoded)
    assert decoded == _expected(original)


@pytest.mark.parametrize(
    "encoder_cls",
    [CustomJSONEncoder, StreamingJSONEncoder],
    ids=["custom", "streaming"],
)
def test_json_encoder_sanitizes_strings_before_serialization(encoder_cls):
    """ESC/CSI/OSC in strings are stripped for both encoders that emit JSON.

    Covers json.dumps (string fast path and C encoder) and json.dump
    (iterencode). Sanitization is observed on the document, not by calling
    JSONEncoder.default() with a str.
    """
    payload = {
        "COL": _CELL,
        "\x1b[31mNAME\x1b[0m": "plain",
        "nested": {"title": _OSC_TITLE, "lines": [_CSI_CLEAR, _OSC8]},
        "plain": "hello",
    }

    _assert_sanitized_json(json.dumps(payload, cls=encoder_cls), payload)

    buffer = StringIO()
    json.dump(payload, buffer, cls=encoder_cls, indent=4)
    _assert_sanitized_json(buffer.getvalue(), payload)

    _assert_sanitized_json(json.dumps(_CELL, cls=encoder_cls), _CELL)
    _assert_sanitized_json(
        json.dumps(ObjectResult({"COL": _CELL}), cls=encoder_cls),
        {"COL": _CELL},
    )
    _assert_sanitized_json(
        json.dumps(MessageResult(_CELL), cls=encoder_cls),
        {"message": _CELL},
    )


def test_custom_encoder_sanitizes_materialized_collection():
    """MultipleResults uses CustomJSONEncoder, which unwraps CollectionResult."""
    result = CollectionResult([{"COL": _CELL}, {"COL": "ok"}])
    encoded = json.dumps(result, cls=CustomJSONEncoder, indent=2)
    _assert_sanitized_json(encoded, [{"COL": _CELL}, {"COL": "ok"}])


@pytest.mark.parametrize(
    "output_format",
    [OutputFormat.JSON, OutputFormat.JSON_EXT],
    ids=["JSON", "JSON_EXT"],
)
def test_print_result_json_formats_sanitize_query_strings(capsys, output_format):
    """snow sql --format JSON and JSON_EXT share the streaming JSON printer."""
    print_result(
        CollectionResult([{"COL": _CELL, "n": 1}]),
        output_format=output_format,
    )
    _assert_sanitized_json(get_output(capsys), [{"COL": _CELL, "n": 1}])


def test_print_result_multiple_results_sanitizes_strings(capsys):
    print_result(
        MultipleResults(
            [
                ObjectResult({"COL": _OSC_TITLE}),
                CollectionResult([{"COL": _CSI_CLEAR}]),
            ]
        ),
        output_format=OutputFormat.JSON,
    )
    _assert_sanitized_json(
        get_output(capsys),
        [{"COL": _OSC_TITLE}, [{"COL": _CSI_CLEAR}]],
    )


def test_print_result_stream_sanitizes_each_record(capsys):
    def records():
        yield MessageResult(_CELL)
        yield ObjectResult({"COL": _CSI_CLEAR})

    print_result(StreamResult(records()), output_format=OutputFormat.JSON)
    lines = [line for line in get_output(capsys).splitlines() if line]
    assert len(lines) == 2
    _assert_sanitized_json(lines[0], {"message": _CELL})
    _assert_sanitized_json(lines[1], {"COL": _CSI_CLEAR})


def test_json_ext_parsed_variant_strings_are_sanitized(capsys, mock_cursor):
    """JSON_EXT json.loads VARIANT text, which turns \\u001b back into ESC."""
    variant = json.dumps({"name": _OSC_TITLE, "clear": _CSI_CLEAR})
    cursor = mock_cursor(
        columns=[{"name": "COL", "type_code": 5}],
        rows=[(variant,)],
    )
    manager = get_cli_context_manager()
    previous = manager.output_format
    manager.output_format = OutputFormat.JSON_EXT
    try:
        print_result(QueryResult(cursor), output_format=OutputFormat.JSON_EXT)
    finally:
        manager.output_format = previous

    _assert_sanitized_json(
        get_output(capsys),
        [{"COL": {"name": _OSC_TITLE, "clear": _CSI_CLEAR}}],
    )


def test_deeply_nested_list_is_sanitized_without_recursion_error():
    """An 800-deep list still encodes, with terminal escapes stripped."""
    value = _CSI_CLEAR
    for _ in range(800):
        value = [value]

    encoded = json.dumps(value, cls=CustomJSONEncoder)
    decoded = json.loads(encoded)
    while isinstance(decoded, list):
        decoded = decoded[0]
    assert decoded == sanitize_for_terminal(_CSI_CLEAR)
    assert "\x1b" not in encoded


def test_deeply_nested_dict_matches_list_nesting_depth():
    """Dict nesting uses one call per level, the same as lists.

    An 800-deep object must encode. That is the depth the list walk already
    guarantees; a second frame per dict level would fail here.
    """
    value = _CSI_CLEAR
    for _ in range(800):
        value = {"k": value}

    encoded = json.dumps(value, cls=CustomJSONEncoder)
    decoded = json.loads(encoded)
    while isinstance(decoded, dict):
        decoded = decoded["k"]
    assert decoded == sanitize_for_terminal(_CSI_CLEAR)
    assert "\x1b" not in encoded


@pytest.mark.parametrize(
    "encoder_cls",
    [CustomJSONEncoder, StreamingJSONEncoder],
    ids=["custom", "streaming"],
)
@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            {"\x1b[31mname\x1b[0m": 1, "name": 2},
            {"name": 1, "name_2": 2},
        ),
        (
            {"\x1b[31mname\x1b[0m": 1, "name": 2, "name_2": 3},
            {"name": 1, "name_3": 2, "name_2": 3},
        ),
        (
            {"\x1b[31mname\x1b[0m": 1, "\x1b[32mname\x1b[0m": 2, "name": 3},
            {"name": 1, "name_2": 2, "name_3": 3},
        ),
        (
            {"\x1b[31m\x1b[0m": "first", "": "second"},
            {"": "first", "_2": "second"},
        ),
        (
            {"\x1b[31mname\x1b[0m": _CSI_CLEAR, "name": "ok"},
            {"name": "", "name_2": "ok"},
        ),
    ],
    ids=["pair", "suffix-taken", "triple", "empty-key", "value-still-stripped"],
)
def test_sanitized_dict_keys_are_uniquified_on_collision(
    encoder_cls, payload, expected
):
    """Keys that differ only by CSI keep every value. The first keeps the name."""
    original = dict(payload)
    encoded = json.dumps(payload, cls=encoder_cls)
    assert payload == original
    assert json.loads(encoded) == expected
    assert "\\u001b" not in encoded
    assert "\x1b" not in encoded


def test_json_encoder_rejects_circular_references():
    payload: dict = {"name": "ok"}
    payload["self"] = payload
    with pytest.raises(ValueError, match="Circular reference detected"):
        json.dumps(payload, cls=CustomJSONEncoder)

    nested: dict = {}
    nested["child"] = {"back": nested}
    with pytest.raises(ValueError, match="Circular reference detected"):
        json.dumps(nested, cls=CustomJSONEncoder)


@pytest.mark.parametrize(
    "encoder_cls",
    [CustomJSONEncoder, StreamingJSONEncoder],
    ids=["custom", "streaming"],
)
def test_json_encoders_preserve_non_ansi_values(encoder_cls):
    payload = {
        "text": "café 🤯",
        "plain": "hello\nworld",
        "count": 2,
        "flag": True,
        "empty": None,
        "when": datetime(2024, 1, 1, 12, 0, 0),
        "amount": Decimal("123.45"),
        "binary": bytearray(b"Hello"),
        "path": Path("/tmp/test"),
        "nested": {"items": ["ok", None, 1]},
    }
    encoded = json.dumps(payload, cls=encoder_cls)
    # ensure_ascii stays on: non-ASCII is escaped, not written as raw UTF-8.
    assert "\\u00e9" in encoded
    assert "🤯" not in encoded
    assert "\x1b" not in encoded
    assert json.loads(encoded) == {
        "text": "café 🤯",
        "plain": "hello\nworld",
        "count": 2,
        "flag": True,
        "empty": None,
        "when": "2024-01-01T12:00:00",
        "amount": "123.45",
        "binary": "48656c6c6f",
        "path": "/tmp/test",
        "nested": {"items": ["ok", None, 1]},
    }
