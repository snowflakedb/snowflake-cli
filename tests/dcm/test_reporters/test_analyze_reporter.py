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
import typer
from snowflake.cli._plugins.dcm import styles
from snowflake.cli._plugins.dcm.reporters.analyze import (
    AnalyzeErrorsReporter,
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


class TestAnalyzeErrorsReporter:
    def _make_response(self, files):
        return {"files": files}

    def _file_error(self, message, line=None, column=0, code=None):
        error: dict = {"message": message}
        if line is not None:
            error["source_position"] = {"line": line, "column": column}
        if code is not None:
            error["code"] = code
        return error

    def _definition(self, name, errors, domain="TABLE", schema=None, database=None):
        definition_id: dict = {"name": name, "domain": domain}
        if schema is not None:
            definition_id["schema"] = schema
        if database is not None:
            definition_id["database"] = database
        return {
            "id": definition_id,
            "refined_domain": domain.lower(),
            "errors": errors,
        }

    def test_process_no_errors_succeeds(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/customers.sql",
                    "definitions": [self._definition("CUSTOMERS", [])],
                    "errors": [],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "Static analysis of DCM Project files found no errors." in output

    def test_process_file_level_errors(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "errors": [
                        self._file_error(
                            "DCM project ANALYZE error: SQL compilation error: "
                            "syntax error line 10 at position 0 unexpected 'defineX'.",
                            line=10,
                            column=0,
                            code="001597",
                        )
                    ],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "sources/definitions/bad.sql" in output
        # We strip the noise prefix and never show the per-finding header
        # (code / line / column / "error" label).
        assert "DCM project ANALYZE error:" not in output
        assert "[001597]" not in output
        assert "line 10:0" not in output
        assert "SQL compilation error: syntax error line 10" in output
        assert (
            "Static analysis of DCM Project files found 1 error and 0 issues." in output
        )

    def test_process_definition_level_errors(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/analytics.sql",
                    "definitions": [
                        self._definition(
                            "ENRICHED_ORDER_DETAILS",
                            [
                                self._file_error(
                                    "Could not analyze lineage due to unresolved dependency",
                                    line=3,
                                    code="001634",
                                ),
                                self._file_error(
                                    "Unresolved or ambiguous dependency",
                                    line=3,
                                    code="001632",
                                ),
                            ],
                            domain="TABLE",
                            schema="ANALYTICS",
                            database="DCM_DEMO_1_DEV3",
                        )
                    ],
                    "errors": [],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "sources/definitions/analytics.sql" in output
        assert "DCM_DEMO_1_DEV3.ANALYTICS.ENRICHED_ORDER_DETAILS (TABLE)" in output
        assert "[001634]" not in output
        assert "[001632]" not in output
        assert "Could not analyze lineage due to unresolved dependency" in output
        assert "Unresolved or ambiguous dependency" in output
        assert (
            "Static analysis of DCM Project files found 2 errors and 0 issues."
            in output
        )

    def test_process_mixed_errors(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/a.sql",
                    "definitions": [
                        self._definition(
                            "TABLE_A",
                            [self._file_error("definition err", line=3, code="001632")],
                        )
                    ],
                    "errors": [self._file_error("file err", line=1, code="001597")],
                },
                {
                    "source_path": "sources/definitions/clean.sql",
                    "definitions": [self._definition("CLEAN", [])],
                    "errors": [],
                },
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "sources/definitions/a.sql" in output
        assert "definition err" in output
        assert "file err" in output
        assert "TABLE_A (TABLE)" in output
        assert "sources/definitions/clean.sql" not in output
        assert (
            "Static analysis of DCM Project files found 2 errors and 0 issues."
            in output
        )

    def test_process_exits_with_code_1_when_errors_present(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "errors": [self._file_error("syntax error", line=1, code="001597")],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(typer.Exit) as exc_info:
                reporter.process(cursor)

        assert exc_info.value.exit_code == 1

    def test_process_does_not_exit_when_only_issues_present(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/note.sql",
                    "definitions": [],
                    "errors": [],
                    "issues": [self._file_error("style nit", line=2, code="W100")],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            reporter.process(cursor)  # Should not raise

    def test_process_handles_error_string(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "errors": ["bare error string"],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "bare error string" in output

    def test_process_handles_legacy_definition_name(self):
        """Definitions without an `id` dict fall back to the legacy `name` field."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/legacy.sql",
                    "definitions": [
                        {
                            "name": "LEGACY_TABLE",
                            "errors": [self._file_error("oops", line=2)],
                        }
                    ],
                    "errors": [],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "LEGACY_TABLE" in output

    def test_process_handles_legacy_source_path(self):
        """`sourcePath` (camelCase) is accepted as a fallback for `source_path`."""
        data = self._make_response(
            [
                {
                    "sourcePath": "sources/definitions/legacy.sql",
                    "definitions": [],
                    "errors": [self._file_error("oops")],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "sources/definitions/legacy.sql" in output

    def test_process_multiline_message(self):
        """Multiline error messages are indented under the error header."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "errors": [
                        self._file_error(
                            "first line\nsecond line",
                            line=1,
                            code="001597",
                        )
                    ],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "first line" in output
        assert "second line" in output

    def test_process_empty_files(self):
        data = self._make_response([])
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "Static analysis of DCM Project files found no errors." in output

    def test_process_no_data(self):
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(None)

        output = capture_reporter_output(reporter, cursor)
        assert "No data." in output

    def test_process_invalid_response_shape(self):
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor({"files": "not-a-list"})

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(CliError) as exc_info:
                reporter.process(cursor)

        assert "Could not process response." in exc_info.value.message

    def test_process_top_level_issues_only_does_not_fail(self):
        """Top-level issues are warnings; they are reported but exit code stays 0."""
        data = {
            "files": [],
            "issues": [
                self._file_error(
                    "deprecated syntax used", line=5, column=2, code="W001"
                )
            ],
        }
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "deprecated syntax used" in output
        # No per-finding header is rendered any more.
        assert "[W001]" not in output
        assert "line 5:2" not in output
        assert (
            "Static analysis of DCM Project files found 0 errors and 1 issue." in output
        )

    def test_process_file_issues_are_warnings(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/note.sql",
                    "definitions": [],
                    "errors": [],
                    "issues": [
                        self._file_error(
                            "consider naming convention",
                            line=2,
                            code="W100",
                        )
                    ],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "sources/definitions/note.sql" in output
        assert "consider naming convention" in output
        assert "[W100]" not in output
        assert (
            "Static analysis of DCM Project files found 0 errors and 1 issue." in output
        )

    def test_process_definition_issues_are_warnings(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/note.sql",
                    "definitions": [
                        {
                            "id": {"name": "T", "domain": "TABLE"},
                            "refined_domain": "table",
                            "errors": [],
                            "issues": [
                                self._file_error("naming nit", line=2, code="W100")
                            ],
                        }
                    ],
                    "errors": [],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "T (TABLE)" in output
        assert "naming nit" in output
        assert "[W100]" not in output

    def test_process_mixed_errors_and_issues_fails_with_combined_summary(self):
        data = {
            "files": [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "errors": [self._file_error("syntax error", line=1, code="001597")],
                    "issues": [self._file_error("style hint", line=2, code="W100")],
                }
            ],
            "issues": [],
        }
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert (
            "Static analysis of DCM Project files found 1 error and 1 issue." in output
        )

        # And the exit code is non-zero (the errors fail the command).
        reporter2 = AnalyzeErrorsReporter()
        cursor2 = FakeCursor(data)
        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(typer.Exit) as exc_info:
                reporter2.process(cursor2)
        assert exc_info.value.exit_code == 1

    def test_source_path_header_uses_file_path_style(self):
        """Source-file headers should render in bold blue for visibility."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "errors": [self._file_error("syntax error", line=1, code="001597")],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)
        calls = []

        def record(text, style=""):
            calls.append((str(text), style))

        with mock.patch(CLI_CONSOLE_PATH, side_effect=record):
            with pytest.raises(typer.Exit):
                reporter.process(cursor)

        path_call = next(c for c in calls if "sources/definitions/bad.sql" in c[0])
        assert path_call[1] == styles.FILE_PATH_STYLE
