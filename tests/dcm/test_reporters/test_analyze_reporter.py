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

    def test_process_fails_when_issues_present(self):
        data = {
            "issues": [{"message": "advisory", "severity": "INFO"}],
            "files": [
                {
                    "source_path": "sources/definitions/orders.sql",
                    "definitions": [
                        {
                            "id": {"name": "ORDERS"},
                            "issues": [
                                {
                                    "message": "unresolved dependency",
                                    "severity": "ERROR",
                                },
                                {
                                    "message": "could not analyze lineage",
                                    "severity": "INFO",
                                },
                            ],
                        },
                        {
                            "id": {"name": "ORDERS_VIEW"},
                            "issues": [
                                {
                                    "message": "conflicting definition",
                                    "severity": "ERROR",
                                }
                            ],
                        },
                    ],
                    "issues": [{"message": "invalid identifier", "severity": "WARN"}],
                },
                {
                    "source_path": "sources/definitions/broken.sql",
                    "definitions": [
                        {
                            "id": {"name": "BROKEN"},
                            "issues": [
                                {"message": "syntax error", "severity": "ERROR"}
                            ],
                        }
                    ],
                    "issues": [{"message": "parse failure", "severity": "ERROR"}],
                },
            ],
        }
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(CliError) as exc_info:
                reporter.process(cursor)

        assert "7 error(s)" in exc_info.value.message

    @pytest.mark.parametrize(
        "data",
        [
            pytest.param({"files": []}, id="empty_files"),
            pytest.param(
                {
                    "files": [
                        {
                            "source_path": "sources/definitions/ok.sql",
                            "definitions": [{"id": {"name": "T"}, "issues": []}],
                            "issues": [],
                        }
                    ],
                    "issues": [],
                },
                id="no_issues",
            ),
        ],
    )
    def test_process_succeeds_when_no_issues(self, data):
        reporter = AnalyzeReporter()

        output = capture_reporter_output(reporter, FakeCursor(data))

        assert "Analysis completed successfully." in output
        assert "error(s)" not in output

    def test_process_no_data(self):
        reporter = AnalyzeReporter()
        cursor = FakeCursor(None)

        output = capture_reporter_output(reporter, cursor)
        assert "No data." in output

    def test_prints_raw_json(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/ok.sql",
                    "definitions": [{"id": {"name": "OK"}, "issues": []}],
                    "issues": [],
                }
            ]
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "sources/definitions/ok.sql" in output
