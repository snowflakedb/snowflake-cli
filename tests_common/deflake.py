from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from subprocess import run, PIPE
from typing import Generator, cast

import pytest
import requests

TEST_TYPE_OPTION = "--deflake-test-type"
PREVIOUS_OUTCOME_KEY = pytest.StashKey[dict[str, str]]()

APP_REPO = "snowflakedb/snowflake-cli"
# ISSUE_REPO = APP_REPO
ISSUE_REPO = "snowflakedb/frank-test"
FLAKY_LABEL = "flaky-test"
PHASES = ["setup", "call", "teardown"]


class DeflakePlugin:
    name = "deflake"

    def pytest_configure(self, config):
        level = logging.DEBUG if config.option.verbose > 1 else logging.INFO
        self.log = logging.getLogger(self.name)
        self.log.setLevel(level)
        self.runner = config.pluginmanager.getplugin("runner")
        self.test_type = config.getoption(TEST_TYPE_OPTION)

        if token := os.getenv("GH_TOKEN"):
            rev_parse = run(["git", "rev-parse", "HEAD"], text=True, stdout=PIPE)
            self.github = GitHub(token, rev_parse.stdout.strip())
        else:
            self.github = None
            self.log.info("GH_TOKEN not provided, issue reporting disabled.")

    def pytest_sessionstart(self, session):
        self.report = Report(root=session.fspath)

    def pytest_runtest_protocol(self, item, nextitem):
        ihook = item.ihook
        ihook.pytest_runtest_logstart(nodeid=item.nodeid, location=item.location)
        os.environ["FORCE_FLAKE"] = "true"
        reports = self.runner.runtestprotocol(item, nextitem=nextitem)
        if any(report.should_retry for report in reports):
            del os.environ["FORCE_FLAKE"]
            self.runner.runtestprotocol(item, nextitem=nextitem)
        ihook.pytest_runtest_logfinish(nodeid=item.nodeid, location=item.location)
        return True

    def pytest_runtest_logstart(self, nodeid, location):
        self.report.tests[nodeid] = TestResult(nodeid)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(
        self, item: pytest.Item, call: pytest.CallInfo
    ) -> pytest.TestReport:
        previous_outcomes = item.stash.setdefault(PREVIOUS_OUTCOME_KEY, {})
        report = (yield).get_result()

        report.should_retry = False
        if call.when in previous_outcomes:
            # This is a retry
            if previous_outcomes[call.when] == "failed" and report.outcome == "passed":
                # This test initially failed then passed on a retry, it's flaky
                report.outcome = "flaky"
        elif report.outcome == "failed":
            # This test should be retried
            report.should_retry = True
            report.wasxfail = "Failure will be retried"

        previous_outcomes[call.when] = report.outcome
        return report

    def pytest_runtest_logreport(self, report):
        test = self.report.tests[report.nodeid]
        phase = getattr(test, report.when)
        phase.outcome = report.outcome

        if report.outcome == "failed":
            test.outcome = "failed"
            phase.longrepr = report.longreprtext
            if crash := getattr(report.longrepr, "reprcrash", None):
                phase.crash = Crash(
                    path=crash.path,
                    lineno=crash.lineno,
                    message=crash.message,
                )
        elif report.outcome == "flaky":
            test.outcome = "flaky"

    @pytest.hookimpl(tryfirst=True)
    def pytest_report_teststatus(self, report, config) -> tuple[str, str, str] | None:
        if report.should_retry:
            # Don't log a status for this yet since it's not final
            return "", "", ""
        if report.outcome == "flaky":
            return "flaky", "K", "FLAKY"
        return None

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus):
        for nodeid, test in self.report.tests.items():
            if test.outcome != "flaky":
                continue

            self.log.debug(nodeid)
            if self.github and self.test_type:
                try:
                    self.github.create_or_update_flaky_test_issue(
                        self.report.root, self.test_type, test
                    )
                except Exception:  # noqa
                    self.log.exception(f"Failed to create or update flaky test issue")


def pytest_configure(config):
    plugin = DeflakePlugin()
    config.pluginmanager.register(plugin, name=plugin.name)


def pytest_addoption(parser):
    group = parser.getgroup(
        "deflake", "deflake tests by recording flakes as GitHub issues"
    )
    group.addoption(
        TEST_TYPE_OPTION, type=str, help="the type of tests being run", default=""
    )


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
    root: Path
    tests: dict[str, TestResult] = field(default_factory=dict)


class GitHub:
    def __init__(self, token: str, sha: str):
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        self.sha = sha

    def create_or_update_flaky_test_issue(
        self, root: Path, test_type: str, test: TestResult
    ) -> None:
        if existing_issue := self.get_issue(test_type, test):
            self.ensure_issue_open(existing_issue)
            self.update_issue_body(existing_issue, root, test)
            self.comment_on_issue(existing_issue, test)
        else:
            self.create_issue(root, test_type, test)

    def get_issue(self, test_type: str, test: TestResult) -> dict[str, dict] | None:
        issues = self.get(f"repos/{ISSUE_REPO}/issues", labels=FLAKY_LABEL, state="all")
        for issue in issues:
            if issue["title"] == self.issue_title(test_type, test):
                return issue
        return None

    def ensure_issue_open(self, issue: dict) -> None:
        if issue["state"] != "open":
            number = issue["number"]
            self.patch(f"repos/{ISSUE_REPO}/issues/{number}", state="open")

    def update_issue_body(self, issue: dict, root: Path, test: TestResult) -> None:
        number = issue["number"]
        body = self.issue_body(root, test)
        self.patch(f"repos/{ISSUE_REPO}/issues/{number}", body=body)

    def comment_on_issue(self, issue: dict, test: TestResult) -> dict:
        body = "+1"
        for phase in PHASES:
            if getattr(test, phase).outcome in ("failed", "flaky"):
                body += f"\n```python\n{getattr(test, phase).crash.message}\n```\n"
                break

        number = issue["number"]
        return cast(
            dict, self.post(f"repos/{ISSUE_REPO}/issues/{number}/comments", body=body)
        )

    def create_issue(self, root: Path, test_type: str, test: TestResult) -> dict:
        title = self.issue_title(test_type, test)
        body = self.issue_body(root, test)
        return cast(
            dict,
            self.post(
                f"repos/{ISSUE_REPO}/issues",
                title=title,
                body=body,
                labels=[FLAKY_LABEL],
            ),
        )

    def issue_title(self, test_type: str, test: TestResult) -> str:
        test_name = self.nodeid_function_name(test.nodeid)
        return f"Flaky {test_type.lower()} test: `{test_name}`"

    def issue_body(self, root: Path, test: TestResult) -> str:
        return "\n".join(
            [f"Nodeid: `{test.nodeid}`"]
            + [
                line
                for phase in PHASES
                for line in self.phase_lines(root, phase, getattr(test, phase))
            ]
        )

    def phase_lines(
        self, root: Path, phase: str, phase_info: TestPhase | None
    ) -> Generator[str, None, None]:
        if phase_info is None:
            return

        if phase_info.outcome == "passed":
            yield f"# 🟢 {phase.title()} passed"
            return

        yield f"# 🔴 {phase.title()} failed"
        yield f"```python\n{phase_info.longrepr}\n```"

        if phase_info.crash:
            crash_info = phase_info.crash
            path = Path(crash_info.path).relative_to(root).as_posix()
            lineno = crash_info.lineno
            url = f"https://github.com/{APP_REPO}/blob/{self.sha}/{path}#L{lineno}"
            yield url

    @staticmethod
    def nodeid_function_name(nodeid: str) -> str:
        return nodeid.split("::", 1)[-1]

    def get(self, path: str, **params) -> dict | list:
        return self.api("get", path, params=params)

    def post(self, path: str, **data) -> dict | list:
        return self.api("post", path, json=data)

    def patch(self, path: str, **data) -> dict | list:
        return self.api("patch", path, json=data)

    def api(self, method, path, *args, **kwargs) -> dict | list:
        assert self.session, "Github API must be configured"
        url = f"https://api.github.com/{path}"
        resp = self.session.request(method, url, *args, **kwargs)
        resp.raise_for_status()
        return resp.json()
