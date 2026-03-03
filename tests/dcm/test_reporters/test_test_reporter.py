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
from unittest import mock

import pytest
from snowflake.cli._plugins.dcm.reporters.test import (
    TestReporter,
    TestRow,
    TestStatus,
)
from snowflake.cli.api.exceptions import CliError

from tests.dcm.test_reporters.utils import (
    CLI_CONSOLE_PATH,
    FakeCursor,
    capture_reporter_output,
)


class TestTestReporter:
    def test_single_passing_expectation(self, snapshot):
        data = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.CUSTOMERS",
                    "expectation_name": "row_count_check",
                    "expectation_violated": False,
                    "expectation_expression": "> 0",
                    "metric_name": "row_count",
                    "value": "1500",
                }
            ]
        }
        output = capture_reporter_output(
            TestReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_single_failing_expectation(self, snapshot):
        data = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.ORDERS",
                    "expectation_name": "null_check",
                    "expectation_violated": True,
                    "expectation_expression": "= 0",
                    "metric_name": "null_count",
                    "value": "15",
                }
            ]
        }
        output = capture_reporter_output(
            TestReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_multiple_expectations_mixed_status(self, snapshot):
        data = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.TABLE_A",
                    "expectation_name": "unique_check",
                    "expectation_violated": False,
                },
                {
                    "table_name": "DB.SCHEMA.TABLE_B",
                    "expectation_name": "not_null",
                    "expectation_violated": True,
                    "expectation_expression": "= 0",
                    "metric_name": "null_count",
                    "value": "42",
                },
                {
                    "table_name": "DB.SCHEMA.TABLE_C",
                    "expectation_name": "range_check",
                    "expectation_violated": False,
                },
            ]
        }
        output = capture_reporter_output(
            TestReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_empty_cursor(self, snapshot):
        output = capture_reporter_output(
            TestReporter(),
            FakeCursor(None),
        )
        assert output == snapshot

    def test_no_expectations(self, snapshot):
        data = {"expectations": []}
        output = capture_reporter_output(
            TestReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_missing_expectations_key(self, snapshot):
        data = {"some_other_key": "value"}
        output = capture_reporter_output(
            TestReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_missing_expectation_violated_field(self, snapshot):
        data = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.UNKNOWN_STATUS",
                    "expectation_name": "some_check",
                }
            ]
        }
        output = capture_reporter_output(
            TestReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_ansi_codes_in_names(self, snapshot):
        data = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.\x1b[31mRED_TABLE\x1b[0m",
                    "expectation_name": "\x1b[32mgreen_check\x1b[0m",
                    "expectation_violated": False,
                }
            ]
        }
        output = capture_reporter_output(
            TestReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_non_dict_entries(self, snapshot):
        data = {
            "expectations": [
                "not_a_dict",
                {
                    "table_name": "DB.SCHEMA.VALID",
                    "expectation_name": "valid_check",
                    "expectation_violated": False,
                },
            ]
        }
        output = capture_reporter_output(
            TestReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_process_raises_cli_error_on_failures(self):
        data = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.FAILED",
                    "expectation_name": "failed_check",
                    "expectation_violated": True,
                }
            ]
        }
        reporter = TestReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(CliError) as exc_info:
                reporter.process(cursor)

        assert "1 failed" in exc_info.value.message

    def test_process_does_not_raise_on_success(self):
        data = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.PASSED",
                    "expectation_name": "passed_check",
                    "expectation_violated": False,
                }
            ]
        }
        reporter = TestReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            reporter.process(cursor)  # Should not raise


class TestTestRow:
    def test_from_dict_with_passing_expectation(self):
        data = {
            "table_name": "MY_TABLE",
            "expectation_name": "my_check",
            "expectation_violated": False,
            "expectation_expression": "> 100",
            "metric_name": "row_count",
            "value": "500",
        }
        row = TestRow.from_dict(data)
        assert row is not None
        assert row.table_name == "MY_TABLE"
        assert row.expectation_name == "my_check"
        assert row.status == TestStatus.PASS
        assert row.expectation_expression == "> 100"
        assert row.metric_name == "row_count"
        assert row.actual_value == "500"

    def test_from_dict_with_failing_expectation(self):
        data = {
            "table_name": "MY_TABLE",
            "expectation_name": "my_check",
            "expectation_violated": True,
        }
        row = TestRow.from_dict(data)
        assert row is not None
        assert row.status == TestStatus.FAIL

    def test_from_dict_with_unknown_status(self):
        data = {
            "table_name": "MY_TABLE",
            "expectation_name": "my_check",
        }
        row = TestRow.from_dict(data)
        assert row is not None
        assert row.status == TestStatus.UNKNOWN

    def test_from_dict_with_non_dict(self):
        row = TestRow.from_dict("not a dict")
        assert row is None

    def test_from_dict_sanitizes_names(self):
        data = {
            "table_name": "TABLE\x1b[31mRED\x1b[0m",
            "expectation_name": "CHECK\x1b[32mGREEN\x1b[0m",
            "expectation_violated": False,
        }
        row = TestRow.from_dict(data)
        assert row is not None
        assert "\x1b" not in row.table_name
        assert "\x1b" not in row.expectation_name
