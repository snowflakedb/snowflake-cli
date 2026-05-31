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
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
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


class Severity(Enum):
    """Severity levels reported by ``EXECUTE DCM PROJECT ... ANALYZE``."""

    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


# Map each severity to its presentation style. Errors are red and fail the run;
# warnings are yellow; info is plain blue (distinct from the bold-blue source
# file headers).
_SEVERITY_STYLE = {
    Severity.ERROR: styles.FAIL_STYLE,
    Severity.WARNING: styles.WARNING_STYLE,
    Severity.INFO: styles.INFO_STYLE,
}


def _normalize_severity(raw: Any) -> Severity:
    """Coerce a free-form ``severity`` value to a :class:`Severity` enum.

    Unknown / missing values are treated as ERROR so a buggy server payload
    never silently downgrades a real problem to a warning or info.
    """
    if isinstance(raw, str):
        value = raw.strip().upper()
        if value in ("ERROR", "ERR", "FATAL"):
            return Severity.ERROR
        if value in ("WARN", "WARNING"):
            return Severity.WARNING
        if value == "INFO":
            return Severity.INFO
    return Severity.ERROR


@dataclass
class _AnalyzeFinding:
    """A single issue pulled from the analyze response."""

    severity: Severity
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


def _to_finding(raw: Any) -> _AnalyzeFinding:
    """Build a :class:`_AnalyzeFinding` from one entry in an ``issues[]`` array."""
    if isinstance(raw, str):
        return _AnalyzeFinding(severity=Severity.ERROR, message=raw)
    if not isinstance(raw, dict):
        return _AnalyzeFinding(severity=Severity.ERROR, message="Unknown issue")

    severity = _normalize_severity(raw.get("severity"))

    message = raw.get("message")
    if not isinstance(message, str) or not message:
        message = "Unknown issue"

    position = raw.get("source_position")
    if not isinstance(position, dict):
        position = {}

    line = position.get("line") if isinstance(position.get("line"), int) else None
    column = position.get("column") if isinstance(position.get("column"), int) else None

    code = raw.get("code")
    if code is not None and not isinstance(code, str):
        code = str(code)

    return _AnalyzeFinding(
        severity=severity, message=message, line=line, column=column, code=code
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

    for raw in file_entry.get("issues") or []:
        collected.file_findings.append(_to_finding(raw))

    for definition in file_entry.get("definitions") or []:
        findings: List[_AnalyzeFinding] = []
        for raw in definition.get("issues") or []:
            findings.append(_to_finding(raw))
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


# Noise prefixes the server prepends to analyze issue messages. We strip them
# so the user sees only the actual diagnostic text.
_NOISE_PREFIXES = ("DCM project ANALYZE error: ",)


def _clean_finding_message(message: str) -> str:
    cleaned = message
    while True:
        for prefix in _NOISE_PREFIXES:
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix) :]
                break
        else:
            return cleaned


def _iter_findings(result_json: Dict[str, Any]) -> Iterator[_AnalyzeFinding]:
    """Yield every issue in the response as an :class:`_AnalyzeFinding`.

    Walks the top-level ``issues[]``, every file's ``issues[]``, and every
    definition's ``issues[]`` once each. This is the single source of truth
    for "how many findings, and at what severity?" — both reporters tally
    off the same stream so their totals can't drift apart.
    """

    def _yield_from(raw_iter: Any) -> Iterator[_AnalyzeFinding]:
        if not isinstance(raw_iter, list):
            return
        for raw in raw_iter:
            yield _to_finding(raw)

    yield from _yield_from(result_json.get("issues"))

    files = result_json.get(_FILES_KEY)
    if not isinstance(files, list):
        return

    for file_entry in files:
        if not isinstance(file_entry, dict):
            continue
        yield from _yield_from(file_entry.get("issues"))
        for definition in file_entry.get("definitions") or []:
            if isinstance(definition, dict):
                yield from _yield_from(definition.get("issues"))


def _tally_by_severity(
    findings: Iterator[_AnalyzeFinding],
) -> Counter:
    """Return a Counter keyed by :class:`Severity` (one entry per bucket).

    The Counter always has explicit zero entries for ERROR/WARNING/INFO so
    callers can read ``counts[Severity.ERROR]`` without a KeyError.
    """
    counts: Counter = Counter({s: 0 for s in Severity})
    for finding in findings:
        counts[finding.severity] += 1
    return counts


class AnalyzeReporter(Reporter[Dict[str, Any]]):
    """Reports the raw JSON returned by ``EXECUTE DCM PROJECT ... ANALYZE``.

    The body is dumped verbatim; only the trailing summary line and the exit
    code react to severity=ERROR findings.
    """

    def __init__(self, save_output: bool = False):
        super().__init__(save_output=save_output)
        self.command_name = "raw-analyze"
        self._error_count = 0

    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        counts = _tally_by_severity(_iter_findings(result_json))
        self._error_count = counts[Severity.ERROR]
        return _files_from_response(result_json)

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        yield from data

    def print_renderables(self, data: Iterator[Dict[str, Any]]) -> None:
        # Drain so any side effects in ``parse_data`` run, but don't print
        # anything per-file: the raw JSON body has already been emitted by
        # the framework via ``result_raw_data``.
        for _ in data:
            pass
        if self.result_raw_data is not None:
            cli_console.styled_message(self.result_raw_data)
            cli_console.styled_message("\n")

    def _generate_summary_renderables(self) -> List[Text]:
        if self._error_count == 0:
            return [
                Text(
                    "Static analysis of DCM Project files found no errors.",
                    styles.PASS_STYLE,
                )
            ]
        errors_word = "error" if self._error_count == 1 else "errors"
        return [
            Text("Static analysis of DCM Project files found "),
            Text(f"{self._error_count} {errors_word}", styles.FAIL_STYLE),
            Text("."),
        ]

    def _is_success(self) -> bool:
        return self._error_count == 0


class AnalyzeErrorsReporter(Reporter[_FileFindings]):
    """Prints a formatted list of issues returned by ``EXECUTE DCM PROJECT ... ANALYZE``.

    Each issue is color-coded by its ``severity`` field:

    * ``ERROR`` — red; triggers a non-zero exit code.
    * ``WARN`` / ``WARNING`` — yellow; informational, exit code unchanged.
    * ``INFO`` — blue; informational, exit code unchanged.

    Unknown severity values are treated as ``ERROR`` so a buggy server payload
    cannot silently downgrade a real problem.
    """

    _INDENT = "  "

    def __init__(self, save_output: bool = False):
        super().__init__(save_output=save_output)
        self.command_name = "analyze"
        self._error_count = 0
        self._warning_count = 0
        self._info_count = 0
        self._top_level_issues: List[_AnalyzeFinding] = []

    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Tally severities off the shared ``_iter_findings`` stream so this
        # reporter and :class:`AnalyzeReporter` can never disagree on the
        # numbers — both walk the response the same way.
        counts = _tally_by_severity(_iter_findings(result_json))
        self._error_count = counts[Severity.ERROR]
        self._warning_count = counts[Severity.WARNING]
        self._info_count = counts[Severity.INFO]

        for raw in result_json.get("issues") or []:
            self._top_level_issues.append(_to_finding(raw))
        return _files_from_response(result_json)

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[_FileFindings]:
        for file_entry in data:
            file_findings = _collect_file_findings(file_entry)
            if file_findings.total == 0:
                continue
            yield file_findings

    def print_renderables(self, data: Iterator[_FileFindings]) -> None:
        first = True
        for file_findings in data:
            if not first:
                cli_console.styled_message("\n")
            first = False

            cli_console.styled_message(
                sanitize_for_terminal(file_findings.source_path),
                style=styles.FILE_PATH_STYLE,
            )
            cli_console.styled_message("\n")

            for finding in file_findings.file_findings:
                self._print_finding(finding, depth=1)

            for group in file_findings.definition_findings:
                self._print_definition_header(group)
                for finding in group.findings:
                    self._print_finding(finding, depth=2)

        for finding in self._top_level_issues:
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
        style = _SEVERITY_STYLE[finding.severity]
        message = sanitize_for_terminal(_clean_finding_message(finding.message))
        for line in message.splitlines() or [""]:
            cli_console.styled_message(f"{indent}{line}", style=style)
            cli_console.styled_message("\n")

    def _generate_summary_renderables(self) -> List[Text]:
        total = self._error_count + self._warning_count + self._info_count
        if total == 0:
            return [
                Text(
                    "Static analysis of DCM Project files found no errors.",
                    styles.PASS_STYLE,
                )
            ]

        # Build (count, label, style) tuples in severity order; only buckets
        # with a non-zero count are included so the summary stays terse.
        segments: List[tuple[int, str, Any]] = []
        if self._error_count > 0:
            label = "error" if self._error_count == 1 else "errors"
            segments.append((self._error_count, label, styles.FAIL_STYLE))
        if self._warning_count > 0:
            label = "warning" if self._warning_count == 1 else "warnings"
            segments.append((self._warning_count, label, styles.WARNING_STYLE))
        if self._info_count > 0:
            label = "info" if self._info_count == 1 else "infos"
            segments.append((self._info_count, label, styles.INFO_STYLE))

        result: List[Text] = [Text("Static analysis of DCM Project files found ")]
        for index, (count, label, style) in enumerate(segments):
            if index > 0:
                # "A and B" for two segments, "A, B, and C" for three (Oxford comma).
                if index == len(segments) - 1:
                    result.append(Text(", and " if len(segments) > 2 else " and "))
                else:
                    result.append(Text(", "))
            result.append(Text(f"{count} {label}", style))
        result.append(Text("."))
        return result

    def _is_success(self) -> bool:
        # Always treat the run as "success" from the base reporter's perspective
        # so that `print_summary()` is always called — we want the styled
        # summary rendered at the end of every run (cf. `snow dcm plan`). The
        # exit code is set separately by ``process_payload``.
        return True

    def process_payload(self, result_json: Dict[str, Any]) -> None:
        super().process_payload(result_json)
        if self._error_count > 0:
            # Errors fail the command, but the styled summary is already on
            # screen — exit silently so we don't double-render the message in
            # an "Error" box.
            raise typer.Exit(code=1)
