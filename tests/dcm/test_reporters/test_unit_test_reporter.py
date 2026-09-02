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
from snowflake.cli._plugins.dcm.reporters.unit_test import (
    UnitTestReporter,
    UnitTestScriptRow,
    UnitTestScriptStatus,
)
from snowflake.cli.api.exceptions import CliError

from tests.dcm.test_reporters.utils import (
    CLI_CONSOLE_PATH,
    FakeCursor,
    capture_reporter_output,
)


class TestUnitTestReporter:
    def test_single_passing_script(self):
        data = {
            "status": "SUCCESSFUL",
            "scripts": [{"script_name": "check_customers", "passed": True}],
        }
        output = capture_reporter_output(UnitTestReporter(), FakeCursor(data))
        assert "check_customers" in output
        assert "PASS" in output
        assert "1 passed" in output
        assert "0 failed" in output

    def test_single_failing_script(self):
        data = {
            "status": "FAILED",
            "scripts": [
                {
                    "script_name": "check_orders",
                    "passed": False,
                    "error_message": "expected 10 rows, got 3",
                }
            ],
        }
        output = capture_reporter_output(UnitTestReporter(), FakeCursor(data))
        assert "check_orders" in output
        assert "FAIL" in output
        assert "expected 10 rows, got 3" in output
        assert "1 failed" in output

    def test_script_with_unrecognized_passed_value_counts_and_renders_as_failure(self):
        """An UNKNOWN status (missing/non-boolean 'passed') is not a clean
        pass, so it must count and render the same as an outright failure -
        never silently as a pass."""
        data = {
            "status": "FAILED",
            "scripts": [{"script_name": "weird_script"}],
        }
        output = capture_reporter_output(UnitTestReporter(), FakeCursor(data))
        assert "weird_script" in output
        assert "FAIL" in output
        assert "PASS" not in output
        assert "1 failed" in output

    def test_multiple_scripts_mixed_status(self):
        data = {
            "status": "FAILED",
            "scripts": [
                {"script_name": "script_a", "passed": True},
                {
                    "script_name": "script_b",
                    "passed": False,
                    "error_message": "boom",
                },
                {"script_name": "script_c", "passed": True},
            ],
        }
        output = capture_reporter_output(UnitTestReporter(), FakeCursor(data))
        assert "script_a" in output
        assert "script_b" in output
        assert "script_c" in output
        assert "2 passed" in output
        assert "1 failed" in output
        assert "3" in output  # total

    def test_empty_cursor(self):
        output = capture_reporter_output(UnitTestReporter(), FakeCursor(None))
        assert output == "No data.\n"

    def test_no_scripts(self):
        data = {"status": "SUCCESSFUL", "scripts": []}
        output = capture_reporter_output(UnitTestReporter(), FakeCursor(data))
        assert "No test scripts were run." in output

    def test_missing_scripts_key(self):
        data = {"status": "SUCCESSFUL"}
        output = capture_reporter_output(UnitTestReporter(), FakeCursor(data))
        assert "No test scripts were run." in output

    def test_non_dict_scripts_value_raises(self):
        data = {"status": "SUCCESSFUL", "scripts": "not_a_list"}
        output = capture_reporter_output(UnitTestReporter(), FakeCursor(data))
        assert "Could not process response." in output

    def test_ansi_codes_in_names_are_stripped(self):
        data = {
            "status": "FAILED",
            "scripts": [
                {
                    "script_name": "script\x1b[31mRED\x1b[0m",
                    "passed": False,
                    "error_message": "err\x1b[32mGREEN\x1b[0m",
                }
            ],
        }
        output = capture_reporter_output(UnitTestReporter(), FakeCursor(data))
        assert "\x1b" not in output

    def test_non_dict_entries_are_skipped(self):
        data = {
            "status": "SUCCESSFUL",
            "scripts": [
                "not_a_dict",
                {"script_name": "valid_script", "passed": True},
            ],
        }
        output = capture_reporter_output(UnitTestReporter(), FakeCursor(data))
        assert "valid_script" in output
        assert "1 passed" in output
        assert "0 failed" in output

    def test_process_raises_cli_error_on_failures(self):
        data = {
            "status": "FAILED",
            "scripts": [
                {"script_name": "broken_script", "passed": False, "error_message": "x"}
            ],
        }
        reporter = UnitTestReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(CliError) as exc_info:
                reporter.process(cursor)

        assert "1 failed" in exc_info.value.message

    def test_process_does_not_raise_on_success(self):
        data = {
            "status": "SUCCESSFUL",
            "scripts": [{"script_name": "ok_script", "passed": True}],
        }
        reporter = UnitTestReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            reporter.process(cursor)  # Should not raise


class TestUnitTestScriptRow:
    def test_from_dict_with_passing_script(self):
        data = {"script_name": "my_script", "passed": True}
        row = UnitTestScriptRow.from_dict(data)
        assert row is not None
        assert row.script_name == "my_script"
        assert row.status == UnitTestScriptStatus.PASS
        assert row.error_message is None

    def test_from_dict_with_failing_script(self):
        data = {
            "script_name": "my_script",
            "passed": False,
            "error_message": "assertion failed",
        }
        row = UnitTestScriptRow.from_dict(data)
        assert row is not None
        assert row.status == UnitTestScriptStatus.FAIL
        assert row.error_message == "assertion failed"

    def test_from_dict_treats_missing_passed_as_unknown(self, caplog):
        data = {"script_name": "my_script"}
        with caplog.at_level("WARNING"):
            row = UnitTestScriptRow.from_dict(data)
        assert row is not None
        assert row.status == UnitTestScriptStatus.UNKNOWN
        assert "my_script" in caplog.text

    def test_from_dict_treats_non_boolean_passed_as_unknown(self, caplog):
        data = {"script_name": "my_script", "passed": "yes"}
        with caplog.at_level("WARNING"):
            row = UnitTestScriptRow.from_dict(data)
        assert row is not None
        assert row.status == UnitTestScriptStatus.UNKNOWN
        assert "my_script" in caplog.text

    def test_from_dict_does_not_warn_on_normal_pass_or_fail(self, caplog):
        with caplog.at_level("WARNING"):
            UnitTestScriptRow.from_dict({"script_name": "a", "passed": True})
            UnitTestScriptRow.from_dict({"script_name": "b", "passed": False})
        assert caplog.text == ""

    def test_from_dict_defaults_script_name_when_missing(self):
        row = UnitTestScriptRow.from_dict({"passed": True})
        assert row is not None
        assert row.script_name == "UNKNOWN"

    def test_from_dict_with_non_dict(self):
        row = UnitTestScriptRow.from_dict("not a dict")
        assert row is None

    def test_from_dict_sanitizes_names(self):
        data = {
            "script_name": "SCRIPT\x1b[31mRED\x1b[0m",
            "passed": False,
            "error_message": "ERR\x1b[32mGREEN\x1b[0m",
        }
        row = UnitTestScriptRow.from_dict(data)
        assert row is not None
        assert "\x1b" not in row.script_name
        assert "\x1b" not in row.error_message
