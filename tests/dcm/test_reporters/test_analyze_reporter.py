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
    Severity,
    _normalize_severity,
)
from snowflake.cli.api.exceptions import CliError

from tests.dcm.test_reporters.utils import (
    CLI_CONSOLE_PATH,
    FakeCursor,
    capture_reporter_output,
)


def _issue(message, severity="ERROR", line=None, column=0, code=None):
    """Build a single issue dict in the new server response shape."""
    issue: dict = {"message": message, "severity": severity}
    if line is not None:
        issue["source_position"] = {"line": line, "column": column}
    if code is not None:
        issue["code"] = code
    return issue


def _definition(name, issues, domain="TABLE", schema=None, database=None):
    """Build a single definition entry with the given issues."""
    definition_id: dict = {"name": name, "domain": domain}
    if schema is not None:
        definition_id["schema"] = schema
    if database is not None:
        definition_id["database"] = database
    return {
        "id": definition_id,
        "refined_domain": domain.lower(),
        "issues": issues,
    }


class TestNormalizeSeverity:
    """Coverage for severity string normalization."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("ERROR", Severity.ERROR),
            ("error", Severity.ERROR),
            ("Err", Severity.ERROR),
            ("FATAL", Severity.ERROR),
            ("WARN", Severity.WARNING),
            ("warning", Severity.WARNING),
            (" Warning ", Severity.WARNING),
            ("INFO", Severity.INFO),
            ("info", Severity.INFO),
        ],
    )
    def test_known_values(self, raw, expected):
        assert _normalize_severity(raw) == expected

    @pytest.mark.parametrize("raw", [None, "", "BOGUS", 42, [], {}])
    def test_unknown_values_default_to_error(self, raw):
        """Unknown / missing severity must NOT silently downgrade the finding."""
        assert _normalize_severity(raw) == Severity.ERROR


class TestAnalyzeReporter:
    """Raw-analyze reporter: dumps JSON body and counts ERROR-severity issues."""

    def _make_response(self, files, top_level_issues=None):
        response = {"files": files}
        if top_level_issues is not None:
            response["issues"] = top_level_issues
        return response

    def test_process_no_errors(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/customers.sql",
                    "definitions": [_definition("CUSTOMERS", [])],
                    "issues": [],
                }
            ]
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            reporter.process(cursor)  # Should not raise

    def test_process_with_file_level_error(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "issues": [_issue("syntax error", severity="ERROR")],
                }
            ]
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(CliError) as exc_info:
                reporter.process(cursor)

        assert "1 error" in exc_info.value.message

    def test_process_with_definition_level_errors(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/customers.sql",
                    "definitions": [
                        _definition(
                            "CUSTOMERS",
                            [
                                _issue("column not found", severity="ERROR"),
                                _issue("type mismatch", severity="ERROR"),
                            ],
                        )
                    ],
                    "issues": [],
                }
            ]
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(CliError) as exc_info:
                reporter.process(cursor)

        assert "2 errors" in exc_info.value.message

    def test_process_with_top_level_errors(self):
        """Top-level issues are also counted in the raw-analyze summary."""
        data = self._make_response(
            files=[],
            top_level_issues=[_issue("project-wide failure", severity="ERROR")],
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(CliError) as exc_info:
                reporter.process(cursor)

        assert "1 error" in exc_info.value.message

    def test_warnings_and_infos_do_not_fail_raw_analyze(self):
        """Only severity=ERROR triggers exit code 1; WARN/INFO are ignored here."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/note.sql",
                    "definitions": [],
                    "issues": [
                        _issue("style nit", severity="WARN"),
                        _issue("hint", severity="INFO"),
                    ],
                }
            ]
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            reporter.process(cursor)  # Should not raise

    def test_process_empty_files(self):
        reporter = AnalyzeReporter()
        cursor = FakeCursor(self._make_response([]))

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
                    "source_path": "sources/definitions/ok.sql",
                    "definitions": [_definition("OK", [])],
                    "issues": [],
                }
            ]
        )
        reporter = AnalyzeReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "sources/definitions/ok.sql" in output


class TestAnalyzeErrorsReporter:
    """User-facing analyze reporter: prints per-file findings color-coded by severity."""

    def _make_response(self, files, top_level_issues=None):
        response = {"files": files}
        if top_level_issues is not None:
            response["issues"] = top_level_issues
        return response

    def test_no_issues_renders_green_success_message(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/customers.sql",
                    "definitions": [_definition("CUSTOMERS", [])],
                    "issues": [],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "Static analysis of DCM Project files found no errors." in output

    def test_file_level_error_is_reported_and_fails(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "issues": [
                        _issue(
                            "DCM project ANALYZE error: SQL compilation error: "
                            "syntax error line 10 at position 0 unexpected 'defineX'.",
                            severity="ERROR",
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
        # Noise prefix is stripped; error code is not rendered; position IS shown.
        assert "DCM project ANALYZE error:" not in output
        assert "[001597]" not in output
        assert "line 10:0:" in output
        assert "SQL compilation error: syntax error line 10" in output
        assert "Static analysis of DCM Project files found 1 error." in output

    def test_definition_level_errors(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/analytics.sql",
                    "definitions": [
                        _definition(
                            "ENRICHED_ORDER_DETAILS",
                            [
                                _issue(
                                    "Could not analyze lineage",
                                    severity="ERROR",
                                    line=3,
                                    code="001634",
                                ),
                                _issue(
                                    "Unresolved or ambiguous dependency",
                                    severity="ERROR",
                                    line=3,
                                    code="001632",
                                ),
                            ],
                            domain="TABLE",
                            schema="ANALYTICS",
                            database="DCM_DEMO_1_DEV3",
                        )
                    ],
                    "issues": [],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "sources/definitions/analytics.sql" in output
        assert "DCM_DEMO_1_DEV3.ANALYTICS.ENRICHED_ORDER_DETAILS (TABLE)" in output
        assert "Could not analyze lineage" in output
        assert "Unresolved or ambiguous dependency" in output
        assert "Static analysis of DCM Project files found 2 errors." in output

    def test_mixed_severities_renders_three_segment_summary(self):
        """Two errors, one warning, three infos → all three buckets shown."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/mixed.sql",
                    "definitions": [
                        _definition(
                            "T",
                            [
                                _issue("err A", severity="ERROR"),
                                _issue("err B", severity="ERROR"),
                                _issue("warn", severity="WARN"),
                                _issue("info A", severity="INFO"),
                                _issue("info B", severity="INFO"),
                                _issue("info C", severity="INFO"),
                            ],
                        )
                    ],
                    "issues": [],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert (
            "Static analysis of DCM Project files found 2 errors, 1 warning, "
            "and 3 infos." in output
        )

    def test_two_segment_summary_uses_and_not_oxford(self):
        """Exactly two non-zero buckets join with ' and ', no Oxford comma."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/mixed.sql",
                    "definitions": [
                        _definition(
                            "T",
                            [
                                _issue("err", severity="ERROR"),
                                _issue("warn", severity="WARN"),
                            ],
                        )
                    ],
                    "issues": [],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert (
            "Static analysis of DCM Project files found 1 error and 1 warning."
            in output
        )

    def test_only_warnings_does_not_fail_command(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/style.sql",
                    "definitions": [],
                    "issues": [_issue("style nit", severity="WARN")],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            reporter.process(cursor)  # Should not raise

    def test_only_infos_does_not_fail_command(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/hints.sql",
                    "definitions": [],
                    "issues": [_issue("hint", severity="INFO")],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            reporter.process(cursor)  # Should not raise

        output = capture_reporter_output(AnalyzeErrorsReporter(), FakeCursor(data))
        assert "Static analysis of DCM Project files found 1 info." in output

    def test_error_present_exits_with_code_1(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "issues": [_issue("syntax error", severity="ERROR")],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(typer.Exit) as exc_info:
                reporter.process(cursor)

        assert exc_info.value.exit_code == 1

    def test_mixed_error_and_warning_exits_with_code_1(self):
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "issues": [
                        _issue("syntax error", severity="ERROR"),
                        _issue("style nit", severity="WARN"),
                    ],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(typer.Exit) as exc_info:
                reporter.process(cursor)
        assert exc_info.value.exit_code == 1

    def test_unknown_severity_is_treated_as_error(self):
        """A buggy server payload must not downgrade a real problem."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/odd.sql",
                    "definitions": [],
                    "issues": [_issue("weird", severity="BOGUS")],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        with mock.patch(CLI_CONSOLE_PATH):
            with pytest.raises(typer.Exit):
                reporter.process(cursor)

    def test_top_level_issues_are_reported(self):
        data = self._make_response(
            files=[],
            top_level_issues=[
                _issue("deprecated syntax used", severity="WARN", line=5, column=2)
            ],
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "deprecated syntax used" in output
        assert "(project-level issue)" in output
        assert "Static analysis of DCM Project files found 1 warning." in output

    def test_process_handles_issue_string(self):
        """Bare strings inside ``issues[]`` are treated as ERROR-severity messages."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "issues": ["bare error string"],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "bare error string" in output
        assert "Static analysis of DCM Project files found 1 error." in output

    def test_process_handles_legacy_definition_name(self):
        """Definitions without an `id` dict fall back to the legacy `name` field."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/legacy.sql",
                    "definitions": [
                        {
                            "name": "LEGACY_TABLE",
                            "issues": [_issue("oops", severity="ERROR", line=2)],
                        }
                    ],
                    "issues": [],
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
                    "issues": [_issue("oops", severity="ERROR")],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "sources/definitions/legacy.sql" in output

    def test_process_multiline_message(self):
        """Multi-line issue messages are indented under their parent."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "issues": [
                        _issue(
                            "first line\nsecond line",
                            severity="ERROR",
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

    def test_position_prefix_shown_when_line_and_column_present(self):
        """Findings with line+column show 'line N:M: ' before the message."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/pos.sql",
                    "definitions": [],
                    "issues": [
                        _issue("type mismatch", severity="ERROR", line=7, column=3)
                    ],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "line 7:3: type mismatch" in output

    def test_position_prefix_line_only_when_column_absent(self):
        """Findings with only a line number show 'line N: ' before the message."""
        issue = {
            "message": "missing ref",
            "severity": "ERROR",
            "source_position": {"line": 5},
        }
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/pos.sql",
                    "definitions": [],
                    "issues": [issue],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "line 5: missing ref" in output

    def test_no_position_prefix_when_line_absent(self):
        """Findings without source_position show just the message with no prefix."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/pos.sql",
                    "definitions": [],
                    "issues": [_issue("no location info", severity="WARNING")],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)

        output = capture_reporter_output(reporter, cursor)
        assert "no location info" in output
        assert "line " not in output

    def test_process_empty_files(self):
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(self._make_response([]))

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

    def test_source_path_header_uses_file_path_style(self):
        """Source-file headers render in bold blue for visibility."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "issues": [_issue("syntax error", severity="ERROR")],
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

    @pytest.mark.parametrize(
        "severity, expected_style",
        [
            ("ERROR", styles.FAIL_STYLE),
            ("WARN", styles.WARNING_STYLE),
            ("WARNING", styles.WARNING_STYLE),
            ("INFO", styles.INFO_STYLE),
        ],
    )
    def test_finding_lines_use_severity_specific_style(self, severity, expected_style):
        """Each finding's message line is styled according to its severity."""
        data = self._make_response(
            [
                {
                    "source_path": "sources/definitions/x.sql",
                    "definitions": [],
                    "issues": [
                        _issue("the diagnostic text", severity=severity, line=3)
                    ],
                }
            ]
        )
        reporter = AnalyzeErrorsReporter()
        cursor = FakeCursor(data)
        calls = []

        def record(text, style=""):
            calls.append((str(text), style))

        with mock.patch(CLI_CONSOLE_PATH, side_effect=record):
            try:
                reporter.process(cursor)
            except typer.Exit:
                pass

        message_call = next(c for c in calls if "the diagnostic text" in c[0])
        assert message_call[1] == expected_style
