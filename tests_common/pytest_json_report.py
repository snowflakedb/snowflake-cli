from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path

OPTION_JSON_FILE = "--json-file"


class JsonReportPlugin:
    def __init__(self):
        super().__init__()
        self.report = None
        self.file_path = None

    def pytest_configure(self, config):
        if file_path := config.getoption(OPTION_JSON_FILE):
            self.file_path = Path(file_path)

    def pytest_sessionstart(self, session):
        self.report = Report(root=str(session.fspath))

    def pytest_runtest_logstart(self, nodeid, location):
        self.report.tests[nodeid] = TestResult(nodeid)

    def pytest_runtest_logreport(self, report):
        nodeid = report.nodeid
        test_result = self.report.tests[nodeid]
        phase = getattr(test_result, report.when)
        phase.outcome = report.outcome

        if report.outcome != "passed":
            test_result.outcome = report.outcome
            phase.longrepr = report.longreprtext
            if crash := getattr(report.longrepr, "reprcrash", None):
                phase.crash = Crash(
                    path=crash.path,
                    lineno=crash.lineno,
                    message=crash.message,
                )

    def pytest_sessionfinish(self, session, exitstatus):
        if self.file_path:
            report = asdict(
                self.report,
                dict_factory=lambda x: {k: v for k, v in x if v is not None},
            )
            with self.file_path.open("w") as f:
                json.dump(report, f, indent=4)


def pytest_configure(config):
    plugin = JsonReportPlugin()
    config.pluginmanager.register(plugin)


def pytest_addoption(parser):
    group = parser.getgroup("json", "emit JSON report")
    group.addoption("--json-file", type=str, help="JSON report file location")


@dataclass(frozen=True)
class Crash:
    path: str
    lineno: int
    message: str


@dataclass
class TestPhase:
    outcome: str = "passed"
    longrepr: str = ""
    crash: Crash | None = field(default=None)


@dataclass
class TestResult:
    nodeid: str
    outcome: str = "passed"
    setup: TestPhase = field(default_factory=TestPhase)
    call: TestPhase = field(default_factory=TestPhase)
    teardown: TestPhase = field(default_factory=TestPhase)


@dataclass(frozen=True)
class Report:
    root: str
    tests: dict[str, TestResult] = field(default_factory=dict)
