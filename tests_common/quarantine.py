# Copyright (c) 2026 Snowflake Inc.
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
"""xfail tests that already fail on main, and only on pull requests.

A listed node id is forgiven after deflake has finished retrying it, unless
the pull request also edits that test's file. Pushes, the daily cron, and
fork ``ok-to-test`` runs leave ``SNOWFLAKE_CLI_CHANGED_FILES`` unset or set
the sentinel, so they stay strict.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from _pytest.reports import TestReport
from _pytest.terminal import TerminalReporter

QUARANTINE_PREFIX = "quarantine: "
UNSET = "__UNSET__"
CHANGED_FILES_ENV = "SNOWFLAKE_CLI_CHANGED_FILES"
QUARANTINE_FILE_ENV = "SNOWFLAKE_CLI_QUARANTINE_FILE"
XPASS_PROPERTY = "snowflake_cli_quarantine"
_DEFAULT_REASON = "listed in tests_common/quarantine.txt"


def quarantine_path() -> Path:
    override = os.environ.get(QUARANTINE_FILE_ENV)
    if override:
        return Path(override)
    return Path(__file__).with_name("quarantine.txt")


def load_entries(path: Path) -> dict[str, str]:
    """Map a node id to the reason printed on its XFAIL line."""
    entries: dict[str, str] = {}
    if not path.is_file():
        return entries
    for raw in path.read_text(encoding="utf-8").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        body, separator, reason = stripped.partition(" #")
        nodeid = body.strip()
        if not nodeid or nodeid in entries:
            continue
        entries[nodeid] = reason.strip() if separator else _DEFAULT_REASON
    return entries


def lookup_reason(nodeid: str, entries: dict[str, str]) -> str | None:
    if nodeid in entries:
        return entries[nodeid]
    # An entry without parameters covers that test's parametrized ids only.
    # ``test_foo`` must not forgive ``test_foo_bar``.
    for entry, reason in entries.items():
        if nodeid.startswith(entry + "["):
            return reason
    return None


def nodeid_path(nodeid: str) -> str:
    return nodeid.split("::", 1)[0].replace("\\", "/")


def parse_changed_files(raw: str) -> set[str]:
    return {
        line.strip().replace("\\", "/") for line in raw.splitlines() if line.strip()
    }


def quarantine_reason(
    nodeid: str,
    *,
    entries: dict[str, str],
    event_name: str | None,
    changed_raw: str | None,
) -> str | None:
    """Return the xfail reason, or None when this run must stay strict."""
    if event_name != "pull_request":
        return None
    # Missing and the workflow sentinel both mean "caller did not opt in".
    # An empty string means the PR changed no files, so listed tests are forgiven.
    if changed_raw is None or changed_raw == UNSET:
        return None
    reason = lookup_reason(nodeid, entries)
    if reason is None:
        return None
    if nodeid_path(nodeid) in parse_changed_files(changed_raw):
        return None
    return reason


@dataclass
class _QuarantineState:
    active: bool = False
    entries: dict[str, str] = field(default_factory=dict)
    changed_raw: str | None = None
    event_name: str | None = None
    xfailed: list[str] = field(default_factory=list)
    xpassed: list[str] = field(default_factory=list)

    def configure(self) -> None:
        self.event_name = os.environ.get("GITHUB_EVENT_NAME")
        self.changed_raw = os.environ.get(CHANGED_FILES_ENV)
        self.active = (
            self.event_name == "pull_request"
            and self.changed_raw is not None
            and self.changed_raw != UNSET
        )
        self.entries = load_entries(quarantine_path()) if self.active else {}
        self.xfailed = []
        self.xpassed = []

    def apply(self, nodeid: str, when: str, report: TestReport) -> None:
        if not self.active:
            return
        # Deflake's makereport wrapper must have already decided should_retry.
        # This hook is tryfirst so it is the outer wrapper and its post-yield
        # runs after deflake's.
        reason = quarantine_reason(
            nodeid,
            entries=self.entries,
            event_name=self.event_name,
            changed_raw=self.changed_raw,
        )
        if reason is None:
            return
        if report.outcome == "failed" and not getattr(report, "should_retry", False):
            report.outcome = "skipped"
            report.wasxfail = f"{QUARANTINE_PREFIX}{reason}"
            return
        if when == "call" and report.outcome in {"passed", "flaky"}:
            report.user_properties.append((XPASS_PROPERTY, "xpass"))

    def note(self, report: TestReport) -> None:
        # xdist workers and the controller both see logreport. The controller
        # copy is the one terminal_summary prints; wasxfail and user_properties
        # are the fields xdist serializes.
        wasxfail = getattr(report, "wasxfail", "") or ""
        if (
            isinstance(wasxfail, str)
            and wasxfail.startswith(QUARANTINE_PREFIX)
            and report.skipped
        ):
            self.xfailed.append(report.nodeid)
            return
        if report.when == "call" and any(
            key == XPASS_PROPERTY and value == "xpass"
            for key, value in report.user_properties
        ):
            self.xpassed.append(report.nodeid)

    def summarize(
        self, terminalreporter: TerminalReporter, config: pytest.Config
    ) -> None:
        if hasattr(config, "workerinput"):
            return
        xfailed = list(dict.fromkeys(self.xfailed))
        xfailed_ids = set(xfailed)
        xpassed = list(dict.fromkeys(n for n in self.xpassed if n not in xfailed_ids))
        if not xfailed and not xpassed:
            return
        terminalreporter.write_sep("=", "snowflake-cli quarantine")
        if xfailed:
            terminalreporter.write_line(
                "XFAIL (already failing on main, test file not in this diff):"
            )
            for nodeid in xfailed:
                terminalreporter.write_line(f"  {nodeid}")
        if xpassed:
            terminalreporter.write_line(
                "XPASS (remove these lines from tests_common/quarantine.txt):"
            )
            for nodeid in xpassed:
                terminalreporter.write_line(f"  {nodeid}")
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if not summary_path:
            return
        lines = ["### snowflake-cli quarantine", ""]
        if xfailed:
            lines.append("XFAIL (already failing on main):")
            lines.extend(f"- `{nodeid}`" for nodeid in xfailed)
            lines.append("")
        if xpassed:
            lines.append("XPASS (delete the quarantine line):")
            lines.extend(f"- `{nodeid}`" for nodeid in xpassed)
            lines.append("")
        with open(summary_path, "a", encoding="utf-8") as handle:
            handle.write("\n".join(lines))


_STATE = _QuarantineState()


def pytest_configure(config: pytest.Config) -> None:
    del config
    _STATE.configure()


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    """Forgive a final failure only after deflake has set ``should_retry``.

    ``tryfirst`` makes this the outer wrapper. Pluggy runs a tryfirst wrapper's
    post-yield after every other wrapper, including deflake, which is registered
    later from ``tests_integration/conftest.py``. Without it the first failure
    is xfailed and the retry never runs.
    """
    outcome = yield
    report: TestReport = outcome.get_result()
    _STATE.apply(item.nodeid, call.when, report)


def pytest_runtest_logreport(report: TestReport) -> None:
    _STATE.note(report)


def pytest_terminal_summary(
    terminalreporter: TerminalReporter,
    exitstatus: int,
    config: pytest.Config,
) -> None:
    del exitstatus
    _STATE.summarize(terminalreporter, config)
