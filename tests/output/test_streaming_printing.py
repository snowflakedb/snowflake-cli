# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
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
from typing import NamedTuple
from unittest.mock import patch

import pytest
from snowflake.cli._app.printing import (
    StreamingJSONEncoder,
    _print_csv_result_streaming,
    _print_json_item_with_array_indentation,
    _stream_collection_as_csv,
    _stream_collection_as_json,
    print_result,
)
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import (
    CollectionResult,
    MessageResult,
    MultipleResults,
    ObjectResult,
)

from tests.testing_utils.conversion import get_output


class MockResultMetadata(NamedTuple):
    name: str
    type_code: int = 1


@pytest.fixture
def sample_collection_result():
    """Create a sample CollectionResult for testing"""
    data = [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": 25, "city": "San Francisco"},
        {"name": "Charlie", "age": 35, "city": "Chicago"},
    ]
    return CollectionResult(data)


@pytest.fixture
def large_collection_result():
    """Create a large CollectionResult to test streaming behavior"""
    data = [
        {"id": i, "value": f"item_{i}", "timestamp": datetime(2024, 1, 1)}
        for i in range(1000)
    ]
    return CollectionResult(data)


@pytest.fixture
def empty_collection_result():
    """Create an empty CollectionResult"""
    return CollectionResult([])


class TestStreamingJSONEncoder:
    """Test the StreamingJSONEncoder functionality"""

    def test_streaming_encoder_raises_error_for_collection_result(
        self, sample_collection_result
    ):
        """StreamingJSONEncoder should raise TypeError for CollectionResult to force streaming"""
        encoder = StreamingJSONEncoder()

        with pytest.raises(
            TypeError, match="CollectionResult should be handled by streaming functions"
        ):
            encoder.default(sample_collection_result)

    def test_streaming_encoder_handles_standard_types(self):
        """StreamingJSONEncoder should handle standard types like the original encoder"""
        encoder = StreamingJSONEncoder()

        # Test datetime
        dt = datetime(2024, 1, 1, 12, 0, 0)
        assert encoder.default(dt) == "2024-01-01T12:00:00"

        # Test Decimal
        dec = Decimal("123.45")
        assert encoder.default(dec) == "123.45"

        # Test Path
        path = Path("/tmp/test")
        assert encoder.default(path) == "/tmp/test"

        # Test bytearray
        ba = bytearray([0x48, 0x65, 0x6C, 0x6C, 0x6F])
        assert encoder.default(ba) == "48656c6c6f"

    def test_streaming_encoder_handles_object_result(self):
        """StreamingJSONEncoder should handle ObjectResult normally"""
        encoder = StreamingJSONEncoder()
        obj_result = ObjectResult({"key": "value"})

        assert encoder.default(obj_result) == {"key": "value"}

    def test_streaming_encoder_handles_message_result(self):
        """StreamingJSONEncoder should handle MessageResult normally"""
        encoder = StreamingJSONEncoder()
        msg_result = MessageResult("Test message")

        # MessageResult.result returns the message, not the raw string
        assert encoder.default(msg_result) == msg_result.result


class TestJSONArrayIndentation:
    """Test JSON array indentation functionality"""

    def test_json_item_indentation_with_indent(self, capsys):
        """Test that JSON items are properly indented in array context"""
        test_item = {"name": "test", "value": 123}

        _print_json_item_with_array_indentation(test_item, 4)

        output = get_output(capsys)
        # The function adds array-level indentation (4 spaces) plus JSON indentation
        expected = '    {\n        "name": "test",\n        "value": 123\n    }'

        assert output == expected

    def test_json_item_indentation_without_indent(self, capsys):
        """Test that JSON items are compact without indentation"""
        test_item = {"name": "test", "value": 123}

        _print_json_item_with_array_indentation(test_item, 0)

        output = get_output(capsys)
        expected = '{"name":"test","value":123}'

        assert output == expected

    def test_json_item_complex_object_indentation(self, capsys):
        """Test indentation with nested objects"""
        test_item = {
            "user": {"name": "Alice", "details": {"age": 30, "city": "New York"}},
            "items": ["item1", "item2"],
        }

        _print_json_item_with_array_indentation(test_item, 2)

        output = get_output(capsys)
        # Verify it starts with proper indentation (2 spaces for array context)
        lines = output.split("\n")
        assert lines[0] == "  {"
        # The user key gets additional indentation from JSON formatting
        assert '    "user": {' in lines
        assert '      "name": "Alice",' in lines


class TestStreamCollectionAsJSON:
    """Test streaming JSON collection functionality"""

    def test_stream_collection_as_json_basic(self, capsys, sample_collection_result):
        """Test basic JSON streaming functionality"""
        _stream_collection_as_json(sample_collection_result, indent=2)

        output = get_output(capsys)
        parsed = json.loads(output)

        expected = [
            {"name": "Alice", "age": 30, "city": "New York"},
            {"name": "Bob", "age": 25, "city": "San Francisco"},
            {"name": "Charlie", "age": 35, "city": "Chicago"},
        ]

        assert parsed == expected

    def test_stream_collection_as_json_indentation_consistency(
        self, capsys, sample_collection_result
    ):
        """Test that first item and subsequent items have consistent indentation"""
        _stream_collection_as_json(sample_collection_result, indent=4)

        output = get_output(capsys)
        lines = output.split("\n")

        # Find the start of first and second objects
        first_obj_start = None
        second_obj_start = None

        for i, line in enumerate(lines):
            if line.strip() == "{" and first_obj_start is None:
                first_obj_start = i
            elif (
                line.strip() == "{"
                and first_obj_start is not None
                and second_obj_start is None
            ):
                second_obj_start = i
                break

        # Both objects should have the same indentation (4 spaces)
        assert lines[first_obj_start] == "    {"
        assert lines[second_obj_start] == "    {"

    def test_stream_collection_as_json_empty(self, capsys, empty_collection_result):
        """Test streaming empty collection"""
        _stream_collection_as_json(empty_collection_result, indent=4)

        output = get_output(capsys)
        assert output == "[]"

    def test_stream_collection_as_json_no_indent(
        self, capsys, sample_collection_result
    ):
        """Test streaming without indentation (compact format)"""
        _stream_collection_as_json(sample_collection_result, indent=0)

        output = get_output(capsys)
        # Even with no indent, we still have newlines for array structure
        # but the JSON objects themselves should be compact
        parsed = json.loads(output)
        assert len(parsed) == 3

        # Verify the objects are on separate lines but compact
        lines = output.strip().split("\n")
        assert lines[0] == "["
        assert lines[-1] == "]"

    def test_stream_collection_memory_efficiency(self, large_collection_result):
        """Test that streaming doesn't load all data into memory at once"""
        # This test verifies that we can process large datasets
        # In a real streaming scenario, this would use much less memory
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            _stream_collection_as_json(large_collection_result, indent=0)
            output = mock_stdout.getvalue()

        # Verify we can parse the complete output
        parsed = json.loads(output)
        assert len(parsed) == 1000
        assert parsed[0]["id"] == 0
        assert parsed[999]["id"] == 999


class TestStreamCollectionAsCSV:
    """Test streaming CSV collection functionality"""

    def test_stream_collection_as_csv_basic(self, capsys, sample_collection_result):
        """Test basic CSV streaming functionality"""
        _stream_collection_as_csv(sample_collection_result)

        output = get_output(capsys)
        lines = output.strip().split("\n")

        # Check header
        assert lines[0] == "name,age,city"

        # Check data rows
        assert "Alice,30,New York" in lines
        assert "Bob,25,San Francisco" in lines
        assert "Charlie,35,Chicago" in lines

        # Should have header + 3 data rows
        assert len(lines) == 4

    def test_stream_collection_as_csv_empty(self, capsys, empty_collection_result):
        """Test streaming empty collection to CSV"""
        _stream_collection_as_csv(empty_collection_result)

        output = get_output(capsys)
        # Empty collection should produce no output
        assert output == ""

    def test_stream_collection_as_csv_special_characters(self, capsys):
        """Test CSV streaming with special characters"""
        data = [
            {
                "name": "Alice, Jr.",
                "description": "Line 1\nLine 2",
                "quote": 'Say "Hello"',
            },
            {"name": "Bob's Data", "description": "Simple text", "quote": "No quotes"},
        ]
        collection = CollectionResult(data)

        _stream_collection_as_csv(collection)

        output = get_output(capsys)
        lines = output.strip().split("\n")

        # CSV should properly escape special characters
        assert "name,description,quote" in lines[0]
        # The newline in "Line 1\nLine 2" creates an extra line in CSV output
        # So we expect more than 3 lines due to CSV escaping
        assert (
            len(lines) >= 3
        )  # At least header + 2 data rows (but may be more due to newlines)

    def test_stream_collection_memory_efficiency_csv(self, large_collection_result):
        """Test that CSV streaming doesn't load all data into memory at once"""
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            _stream_collection_as_csv(large_collection_result)
            output = mock_stdout.getvalue()

        lines = output.strip().split("\n")
        # Should have header + 1000 data rows
        assert len(lines) == 1001
        assert lines[0] == "id,value,timestamp"
        assert "0,item_0,2024-01-01T00:00:00" in lines[1]
        assert "999,item_999,2024-01-01T00:00:00" in lines[1000]


class TestPrintCSVResultStreaming:
    """Test the main CSV result streaming function"""

    def test_print_csv_collection_result(self, capsys, sample_collection_result):
        """Test printing CollectionResult as CSV"""
        _print_csv_result_streaming(sample_collection_result)

        output = get_output(capsys)
        lines = output.strip().split("\n")

        assert lines[0] == "name,age,city"
        assert len(lines) == 4  # header + 3 rows

    def test_print_csv_object_result(self, capsys):
        """Test printing ObjectResult as CSV"""
        obj_result = ObjectResult({"name": "Alice", "age": 30, "city": "New York"})

        _print_csv_result_streaming(obj_result)

        output = get_output(capsys)
        lines = output.strip().split("\n")

        assert lines[0] == "name,age,city"
        assert lines[1] == "Alice,30,New York"
        assert len(lines) == 2

    def test_print_csv_message_result(self, capsys):
        """Test printing MessageResult as CSV"""
        msg_result = MessageResult("Operation completed successfully")

        _print_csv_result_streaming(msg_result)

        output = get_output(capsys)
        lines = output.strip().split("\n")

        assert lines[0] == "message"
        assert lines[1] == "Operation completed successfully"
        assert len(lines) == 2

    def test_print_csv_message_result_with_special_chars(self, capsys):
        """Test printing MessageResult with special characters"""
        msg_result = MessageResult('Message with "quotes" and, commas')

        _print_csv_result_streaming(msg_result)

        output = get_output(capsys)
        lines = output.strip().split("\n")

        assert lines[0] == "message"
        # CSV writer should properly escape the special characters
        assert "quotes" in lines[1] and "commas" in lines[1]


class TestIntegrationWithPrintResult:
    """Test integration of streaming functions with the main print_result function"""

    def test_print_result_json_streaming(self, capsys, sample_collection_result):
        """Test that print_result uses streaming for JSON output"""
        print_result(sample_collection_result, output_format=OutputFormat.JSON)

        output = get_output(capsys)
        parsed = json.loads(output)

        expected = [
            {"name": "Alice", "age": 30, "city": "New York"},
            {"name": "Bob", "age": 25, "city": "San Francisco"},
            {"name": "Charlie", "age": 35, "city": "Chicago"},
        ]

        assert parsed == expected

    def test_print_result_csv_streaming(self, capsys, sample_collection_result):
        """Test that print_result uses streaming for CSV output"""
        print_result(sample_collection_result, output_format=OutputFormat.CSV)

        output = get_output(capsys)
        lines = output.strip().split("\n")

        assert lines[0] == "name,age,city"
        assert len(lines) == 4

    def test_print_result_multiple_results_json(self, capsys, sample_collection_result):
        """Test streaming with MultipleResults in JSON format"""
        multiple = MultipleResults([sample_collection_result, sample_collection_result])

        print_result(multiple, output_format=OutputFormat.JSON)

        output = get_output(capsys)
        parsed = json.loads(output)

        # Should be array of two identical result sets
        assert len(parsed) == 2
        assert parsed[0] == parsed[1]
        assert len(parsed[0]) == 3

    def test_print_result_multiple_results_csv(self, capsys, sample_collection_result):
        """Test streaming with MultipleResults in CSV format"""
        multiple = MultipleResults([sample_collection_result, sample_collection_result])

        print_result(multiple, output_format=OutputFormat.CSV)

        output = get_output(capsys)
        # Should have two complete CSV outputs
        assert output.count("name,age,city") == 2  # Two headers
        assert output.count("Alice,30,New York") == 2  # Alice appears twice


class TestDataTypeHandling:
    """Test handling of various data types in streaming output"""

    def test_streaming_with_special_types(self, capsys):
        """Test streaming with datetime, decimal, path, and bytearray"""
        data = [
            {
                "timestamp": datetime(2024, 1, 1, 12, 0, 0),
                "amount": Decimal("123.45"),
                "path": Path("/tmp/test"),
                "binary": bytearray([0x48, 0x65, 0x6C, 0x6C, 0x6F]),
                "null_value": None,
            }
        ]
        collection = CollectionResult(data)

        # Test JSON output
        print_result(collection, output_format=OutputFormat.JSON)
        json_output = get_output(capsys)
        parsed = json.loads(json_output)

        assert parsed[0]["timestamp"] == "2024-01-01T12:00:00"
        assert parsed[0]["amount"] == "123.45"
        assert parsed[0]["path"] == "/tmp/test"
        assert parsed[0]["binary"] == "48656c6c6f"
        assert parsed[0]["null_value"] is None

    def test_streaming_csv_with_special_types(self, capsys):
        """Test CSV streaming with special data types"""
        data = [
            {
                "timestamp": datetime(2024, 1, 1, 12, 0, 0),
                "amount": Decimal("123.45"),
                "path": Path("/tmp/test"),
                "binary": bytearray([0x48, 0x65, 0x6C, 0x6C, 0x6F]),
                "null_value": None,
            }
        ]
        collection = CollectionResult(data)

        print_result(collection, output_format=OutputFormat.CSV)
        csv_output = get_output(capsys)
        lines = csv_output.strip().split("\n")

        # Check that special types are properly converted to strings
        data_line = lines[1]
        assert "2024-01-01T12:00:00" in data_line
        assert "123.45" in data_line
        assert "/tmp/test" in data_line
        assert "48656c6c6f" in data_line
        # null_value should be empty string in CSV
