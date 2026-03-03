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
from snowflake.cli._plugins.dcm.reporters.analyze import (
    AnalyzeReporter,
)
from snowflake.cli.api.exceptions import CliError

from tests.dcm.test_reporters.utils import (
    CLI_CONSOLE_PATH,
    FakeCursor,
    capture_reporter_output,
)


class TestAnalyzeReporter:
    def _make_response(self, files):
        return {"files": files}

    def test_process_no_errors(self):
        data = self._make_response(
            [
                {
                    "sourcePath": "sources/definitions/customers.sql",
                    "definitions": [{"name": "CUSTOMERS", "errors": []}],
                    "errors": [],
                }
            ]
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            reporter.process(cursor)  # Should not raise

    def test_process_with_file_errors(self):
        data = self._make_response(
            [
                {
                    "sourcePath": "sources/definitions/bad.sql",
                    "definitions": [],
                    "errors": [{"message": "syntax error"}],
                }
            ]
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(CliError) as exc_info:
                reporter.process(cursor)

        assert "1 error(s)" in exc_info.value.message

    def test_process_with_definition_errors(self):
        data = self._make_response(
            [
                {
                    "sourcePath": "sources/definitions/customers.sql",
                    "definitions": [
                        {
                            "name": "CUSTOMERS",
                            "errors": [
                                {"message": "column not found"},
                                {"message": "type mismatch"},
                            ],
                        }
                    ],
                    "errors": [],
                }
            ]
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(CliError) as exc_info:
                reporter.process(cursor)

        assert "2 error(s)" in exc_info.value.message

    def test_process_with_mixed_errors(self):
        data = self._make_response(
            [
                {
                    "sourcePath": "sources/definitions/a.sql",
                    "definitions": [{"name": "A", "errors": [{"message": "err1"}]}],
                    "errors": [{"message": "file err"}],
                },
                {
                    "sourcePath": "sources/definitions/b.sql",
                    "definitions": [{"name": "B", "errors": []}],
                    "errors": [],
                },
            ]
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(CliError) as exc_info:
                reporter.process(cursor)

        assert "2 error(s)" in exc_info.value.message

    def test_process_empty_files(self):
        data = self._make_response([])
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            reporter.process(cursor)  # Should not raise

    def test_process_no_data(self):
        reporter = AnalyzeReporter()
        cursor = FakeCursor(None)

        output = capture_reporter_output(reporter, cursor)
        assert "No data." in output

    def test_prints_raw_json(self):
        data = self._make_response(
            [
                {
                    "sourcePath": "sources/definitions/ok.sql",
                    "definitions": [{"name": "OK", "errors": []}],
                    "errors": [],
                }
            ]
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "sources/definitions/ok.sql" in output
