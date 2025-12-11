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
from io import StringIO
from unittest import mock

import pytest
from snowflake.cli._plugins.dcm.reporters import RefreshReporter, RefreshRow


class FakeCursor:
    """Fake cursor that returns JSON data like a real Snowflake cursor."""

    def __init__(self, data):
        self._data = data
        self._fetched = False

    def fetchone(self):
        if self._fetched:
            return None
        self._fetched = True
        if self._data is None:
            return None
        return (json.dumps(self._data) if isinstance(self._data, dict) else self._data,)


def capture_reporter_output(reporter, cursor):
    """Capture the output from a reporter's process method."""
    output = StringIO()

    def mock_print(text, style=""):
        if hasattr(text, "plain"):
            output.write(text.plain)
        else:
            output.write(str(text))

    with mock.patch(
        "snowflake.cli._plugins.dcm.reporters.cli_console.styled_message",
        side_effect=mock_print,
    ):
        reporter.process(cursor)

    return output.getvalue()


class TestRefreshReporter:
    def test_single_refreshed_table(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.CUSTOMERS",
                    "statistics": '{"insertedRows": 1500, "deletedRows": 200}',
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_single_up_to_date_table(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.ORDERS",
                    "statistics": "No new data",
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_multiple_tables_mixed_status(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.TABLE_A",
                    "statistics": '{"insertedRows": 50000, "deletedRows": 1000}',
                },
                {
                    "dt_name": "DB.SCHEMA.TABLE_B",
                    "statistics": "No new data",
                },
                {
                    "dt_name": "DB.SCHEMA.TABLE_C",
                    "statistics": '{"insertedRows": 0, "deletedRows": 0}',
                },
                {
                    "dt_name": "DB.SCHEMA.TABLE_D",
                    "statistics": '{"insertedRows": 999999, "deletedRows": 500}',
                },
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_large_numbers(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.BILLIONS",
                    "statistics": '{"insertedRows": 1500000000, "deletedRows": 999999999}',
                },
                {
                    "dt_name": "DB.SCHEMA.TRILLIONS",
                    "statistics": '{"insertedRows": 2500000000000, "deletedRows": 100000000000}',
                },
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_statistics_as_dict(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.DICT_STATS",
                    "statistics": {"insertedRows": 100, "deletedRows": 50},
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_only_insertions(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.INSERTS_ONLY",
                    "statistics": '{"insertedRows": 5000}',
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_only_deletions(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.DELETES_ONLY",
                    "statistics": '{"deletedRows": 3000}',
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_empty_cursor(self, snapshot):
        output = capture_reporter_output(RefreshReporter(), FakeCursor(None))
        assert output == snapshot

    def test_no_dynamic_tables(self, snapshot):
        data = {"refreshed_tables": []}
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_missing_refreshed_tables_key(self, snapshot):
        data = {"some_other_key": "value"}
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_malformed_statistics_json(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.BAD_JSON",
                    "statistics": "{invalid_json",
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_unknown_statistics_format(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.WEIRD_STATS",
                    "statistics": "some unexpected string",
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_null_values_in_statistics(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.NULL_STATS",
                    "statistics": '{"insertedRows": null, "deletedRows": null}',
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_missing_dt_name(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "statistics": '{"insertedRows": 100}',
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_non_dict_table_entries(self, snapshot):
        data = {
            "refreshed_tables": [
                "not_a_dict",
                {"dt_name": "DB.SCHEMA.VALID", "statistics": "No new data"},
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_string_numbers_in_statistics(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.STRING_NUMS",
                    "statistics": '{"insertedRows": "500", "deletedRows": "100"}',
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_ansi_codes_in_table_name(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.\x1b[31mRED_TABLE\x1b[0m",
                    "statistics": "No new data",
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot


class TestRefreshRowFormatNumber:
    """Unit tests for number formatting edge cases."""

    @pytest.mark.parametrize(
        "input_num, expected",
        [
            (0, "0"),
            (999, "999"),
            (1000, "1k"),
            (1500, "1.5k"),
            (999999, "999k"),
            (1000000, "1M"),
            (999999999, "999M"),
            (1000000000, "1B"),
            (999999999999, "999B"),
            (1000000000000, "1T"),
            (999999999999999, "999T"),
            (1000000000000000, "1P"),
            (1000000000000000000, "1E"),
        ],
    )
    def test_format_number_boundaries(self, input_num, expected):
        assert RefreshRow._format_number(input_num) == expected  # noqa: SLF001
