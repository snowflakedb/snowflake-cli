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
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional

import typer
from rich.text import Text
from snowflake.cli._plugins.dcm import styles
from snowflake.cli._plugins.dcm.reporters.base import Reporter, cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sanitizers import sanitize_for_terminal

log = logging.getLogger(__name__)

_FILES_KEY = "files"


def _files_from_response(result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    files = result_json.get(_FILES_KEY, [])
    if not isinstance(files, list):
        log.info('Unexpected response format. Expected "files" to be a list: %s', files)
        raise CliError("Could not process response.")
    return files


class AnalyzeReporter(Reporter[Dict[str, Any]]):
    def __init__(self, save_output: bool = False):
        super().__init__(save_output=save_output)
        self.command_name = "raw-analyze"
        self._error_count = 0

    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        return _files_from_response(result_json)

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        for file_entry in data:
            self._error_count += len(file_entry.get("errors", []))
            for definition in file_entry.get("definitions", []):
                self._error_count += len(definition.get("errors", []))
            yield file_entry

    def print_renderables(self, data: Iterator[Dict[str, Any]]) -> None:
        for _ in data:
            pass
        if self.result_raw_data is not None:
            cli_console.styled_message(self.result_raw_data)
            cli_console.styled_message("\n")

    def _generate_summary_renderables(self) -> List[Text]:
        if self._error_count == 0:
            return [Text("Analysis completed successfully.")]
        return [Text(f"Analysis found {self._error_count} error(s).")]

    def _is_success(self) -> bool:
        return self._error_count == 0


_KIND_ERROR = "error"
_KIND_ISSUE = "issue"


@dataclass
class _AnalyzeFinding:
    """A single error or issue pulled from the analyze response."""

    kind: str  # _KIND_ERROR or _KIND_ISSUE
    message: str
    line: Optional[int] = None
    column: Optional[int] = None
    code: Optional[str] = None


@dataclass
class _DefinitionFindings:
    """Findings attached to a single definition inside a file."""

    definition_id: Optional[str]
    definition_domain: Optional[str]
    findings: List[_AnalyzeFinding] = field(default_factory=list)


@dataclass
class _FileFindings:
    """All findings collected for a single source file."""

    source_path: str
    file_findings: List[_AnalyzeFinding] = field(default_factory=list)
    definition_findings: List[_DefinitionFindings] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.file_findings) + sum(
            len(group.findings) for group in self.definition_findings
        )


def _to_finding(raw: Any, kind: str) -> _AnalyzeFinding:
    if isinstance(raw, str):
        return _AnalyzeFinding(kind=kind, message=raw)
    if not isinstance(raw, dict):
        return _AnalyzeFinding(kind=kind, message=f"Unknown {kind}")

    message = raw.get("message")
    if not isinstance(message, str) or not message:
        message = f"Unknown {kind}"

    position = raw.get("source_position")
    if not isinstance(position, dict):
        position = {}

    line = position.get("line") if isinstance(position.get("line"), int) else None
    column = position.get("column") if isinstance(position.get("column"), int) else None

    code = raw.get("code")
    if code is not None and not isinstance(code, str):
        code = str(code)

    return _AnalyzeFinding(
        kind=kind, message=message, line=line, column=column, code=code
    )


def _format_definition_identifier(definition: Dict[str, Any]) -> Optional[str]:
    """Build a dotted identifier from a definition's `id` (or legacy `name`)."""
    definition_id = definition.get("id")
    if isinstance(definition_id, dict):
        name = definition_id.get("name")
        if not isinstance(name, str) or not name:
            return None
        parts: List[str] = []
        database = definition_id.get("database")
        schema = definition_id.get("schema")
        if isinstance(database, str) and database:
            parts.append(database)
        if isinstance(schema, str) and schema:
            parts.append(schema)
        parts.append(name)
        return ".".join(parts)
    legacy_name = definition.get("name")
    if isinstance(legacy_name, str) and legacy_name:
        return legacy_name
    return None


def _format_definition_domain(definition: Dict[str, Any]) -> Optional[str]:
    refined = definition.get("refined_domain")
    if isinstance(refined, str) and refined:
        return refined.upper()
    definition_id = definition.get("id")
    if isinstance(definition_id, dict):
        domain = definition_id.get("domain")
        if isinstance(domain, str) and domain:
            return domain.upper()
    return None


def _collect_file_findings(file_entry: Dict[str, Any]) -> _FileFindings:
    source_path = file_entry.get("source_path") or file_entry.get("sourcePath")
    if not isinstance(source_path, str) or not source_path:
        source_path = "<unknown>"
    collected = _FileFindings(source_path=source_path)

    for raw in file_entry.get("errors") or []:
        collected.file_findings.append(_to_finding(raw, _KIND_ERROR))
    for raw in file_entry.get("issues") or []:
        collected.file_findings.append(_to_finding(raw, _KIND_ISSUE))

    for definition in file_entry.get("definitions") or []:
        findings: List[_AnalyzeFinding] = []
        for raw in definition.get("errors") or []:
            findings.append(_to_finding(raw, _KIND_ERROR))
        for raw in definition.get("issues") or []:
            findings.append(_to_finding(raw, _KIND_ISSUE))
        if not findings:
            continue
        collected.definition_findings.append(
            _DefinitionFindings(
                definition_id=_format_definition_identifier(definition),
                definition_domain=_format_definition_domain(definition),
                findings=findings,
            )
        )

    return collected


# Noise prefixes the server prepends to every analyze error message. We strip
# them so the user sees only the actual diagnostic text.
_NOISE_PREFIXES = ("DCM project ANALYZE error: ",)


def _clean_finding_message(message: str) -> str:
    cleaned = message
    # Strip repeatedly in case a prefix appears more than once.
    while True:
        for prefix in _NOISE_PREFIXES:
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix) :]
                break
        else:
            return cleaned


class AnalyzeErrorsReporter(Reporter[_FileFindings]):
    """Prints a formatted list of errors and issues found by `EXECUTE DCM PROJECT ... ANALYZE`.

    Errors are rendered in red and trigger a non-zero exit code; issues are
    rendered in yellow as warnings and do not change the exit code on their own.
    """

    _INDENT = "  "

    def __init__(self, save_output: bool = False):
        super().__init__(save_output=save_output)
        self.command_name = "analyze"
        self._error_count = 0
        self._issue_count = 0
        self._top_level_issues: List[_AnalyzeFinding] = []

    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        for raw in result_json.get("issues") or []:
            self._top_level_issues.append(_to_finding(raw, _KIND_ISSUE))
        return _files_from_response(result_json)

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[_FileFindings]:
        for file_entry in data:
            file_findings = _collect_file_findings(file_entry)
            if file_findings.total == 0:
                continue
            for finding in file_findings.file_findings:
                self._tally(finding)
            for group in file_findings.definition_findings:
                for finding in group.findings:
                    self._tally(finding)
            yield file_findings

    def _tally(self, finding: _AnalyzeFinding) -> None:
        if finding.kind == _KIND_ERROR:
            self._error_count += 1
        else:
            self._issue_count += 1

    def print_renderables(self, data: Iterator[_FileFindings]) -> None:
        first = True
        for file_findings in data:
            if not first:
                cli_console.styled_message("\n")
            first = False

            cli_console.styled_message(
                sanitize_for_terminal(file_findings.source_path),
                style=styles.BOLD_STYLE,
            )
            cli_console.styled_message("\n")

            for finding in file_findings.file_findings:
                self._print_finding(finding, depth=1)

            for group in file_findings.definition_findings:
                self._print_definition_header(group)
                for finding in group.findings:
                    self._print_finding(finding, depth=2)

        for finding in self._top_level_issues:
            self._issue_count += 1
            if not first:
                cli_console.styled_message("\n")
            first = False
            cli_console.styled_message("(project-level issue)", style=styles.BOLD_STYLE)
            cli_console.styled_message("\n")
            self._print_finding(finding, depth=1)

    def _print_definition_header(self, group: _DefinitionFindings) -> None:
        identifier = group.definition_id or "<unnamed definition>"
        header = sanitize_for_terminal(identifier)
        if group.definition_domain:
            header = f"{header} ({sanitize_for_terminal(group.definition_domain)})"
        cli_console.styled_message(self._INDENT + header)
        cli_console.styled_message("\n")

    def _print_finding(self, finding: _AnalyzeFinding, depth: int) -> None:
        indent = self._INDENT * depth
        style = (
            styles.FAIL_STYLE if finding.kind == _KIND_ERROR else styles.WARNING_STYLE
        )
        message = sanitize_for_terminal(_clean_finding_message(finding.message))
        for line in message.splitlines() or [""]:
            cli_console.styled_message(f"{indent}{line}", style=style)
            cli_console.styled_message("\n")

    def _generate_summary_renderables(self) -> List[Text]:
        if self._error_count == 0 and self._issue_count == 0:
            return [Text("Analysis completed successfully.", styles.PASS_STYLE)]

        errors_plural = "error" if self._error_count == 1 else "errors"
        issues_plural = "issue" if self._issue_count == 1 else "issues"
        return [
            Text("Analysis found "),
            Text(f"{self._error_count} {errors_plural}", styles.FAIL_STYLE),
            Text(" and "),
            Text(f"{self._issue_count} {issues_plural}", styles.WARNING_STYLE),
            Text("."),
        ]

    def _is_success(self) -> bool:
        # Always treat the run as "success" from the base reporter's perspective
        # so that `print_summary()` is always called — we want the summary line
        # rendered at the end of every run (cf. `snow dcm plan`). The exit code
        # is set separately by `process_payload`.
        return True

    def process_payload(self, result_json: Dict[str, Any]) -> None:
        super().process_payload(result_json)
        if self._error_count > 0:
            # Errors fail the command, but the styled summary is already on
            # screen — exit silently so we don't double-render the message in
            # an "Error" box.
            raise typer.Exit(code=1)
