from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from subprocess import run, PIPE
from typing import Generator, cast

import pytest
import requests
import pluggy
from _pytest import runner
from _pytest.config import Config, Parser
from _pytest.main import Session
from _pytest.nodes import Item
from _pytest.reports import TestReport
from _pytest.runner import CallInfo
from _pytest.stash import StashKey
from _pytest.terminal import TerminalReporter

TEST_TYPE_OPTION = "--deflake-test-type"
PREVIOUS_OUTCOME_KEY = StashKey[dict[str, str]]()

FAILED = "failed"
FLAKY = "flaky"

APP_REPO = "snowflakedb/snowflake-cli"
ISSUE_REPO = APP_REPO
FLAKY_LABEL = "flaky-test"
PHASES = ["setup", "call", "teardown"]


class DeflakePlugin:
    """Pytest plugin to retry tests and mark them as flaky.

    This plugin is used to determine which tests are flaky by automatically
    retrying them and marking them as "flaky" instead of "passing" if they
    pass on the retry. After the test run, a GitHub issue is opened or updated
    for each flaky test encountered.
    """

    name = "deflake"

    def __init__(self: DeflakePlugin, config: Config) -> None:
        super().__init__()
        self.exceptions: dict[str, Exception] = {}
        self.flaky_tests = 0
        self.test_run = TestRun()

        self.runner: type[runner] = config.pluginmanager.getplugin("runner")
        self.test_type: str = config.getoption(TEST_TYPE_OPTION)

        if token := os.getenv("GH_TOKEN"):
            # Grab the current commit so we can generate absolute URLs
            rev_parse = run(["git", "rev-parse", "HEAD"], text=True, stdout=PIPE)
            self.github: GitHub | None = GitHub(token, rev_parse.stdout.strip())
        else:
            self.github = None

    def pytest_sessionstart(self, session: Session) -> None:
        # The session is the root node in pytest's collection tree
        # Its path is the root directory that pytest looks in for tests, in
        # our case it's the root of the repo, so we can use it to make
        # relative paths that can be used to generate GitHub urls
        self.test_run.root = session.path

    def pytest_runtest_protocol(self, item: Item, nextitem: Item | None) -> bool:
        # The main protocol for running a single test
        # This function was adapted from pytest internals since we
        # need to override it completely to retry a test

        ihook = item.ihook

        # Call the logstart hook, which typically prints the name of the tests
        ihook.pytest_runtest_logstart(nodeid=item.nodeid, location=item.location)

        # Delegate back to the pytest runner to run all the phases for this test
        reports = self.runner.runtestprotocol(item, nextitem=nextitem)

        # Retry if the test reports that it should retry (report.should_retry is
        # set by us in pytest_runtest_makereport below)
        if any(getattr(report, "should_retry", False) for report in reports):
            self.runner.runtestprotocol(item, nextitem=nextitem)

        # Close the log line for this test
        ihook.pytest_runtest_logfinish(nodeid=item.nodeid, location=item.location)

        # Just return something so the next hook implementation isn't called
        return True

    def pytest_runtest_logstart(self, nodeid: str):
        # This is called once before each test regardless of the number of retries
        self.test_run.tests[nodeid] = TestResult(nodeid)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self, item: Item, call: CallInfo) -> TestReport:
        # Wrap the creation of the test report for this phase to override the
        # status to "flaky" if it's a retry and it passed
        previous_outcomes = item.stash.setdefault(PREVIOUS_OUTCOME_KEY, {})

        # yield nothing, receive result when generator resumes
        result_wrapper: pluggy.Result = yield
        report = result_wrapper.get_result()

        report.should_retry = False
        if call.when in previous_outcomes:
            # This is a retry
            if previous_outcomes[call.when] == FAILED and report.outcome == "passed":
                # This test initially failed then passed on a retry, it's flaky
                report.outcome = FLAKY
                self.flaky_tests += 1
        elif report.outcome == FAILED:
            # This test should be retried
            report.should_retry = True
            # Don't count this as an error
            report.wasxfail = "Failure will be retried"

        previous_outcomes[call.when] = report.outcome
        return report

    def pytest_runtest_logreport(self, report: TestReport) -> None:
        # Record the crash data for the GitHub issue
        # and set the status of the overall test result
        test = self.test_run.tests[report.nodeid]
        phase = getattr(test, report.when)
        phase.outcome = report.outcome

        if report.outcome == FAILED:
            test.outcome = FAILED
            phase.longrepr = report.longreprtext
            if crash := getattr(report.longrepr, "reprcrash", None):
                phase.crash = Crash(
                    path=crash.path,
                    lineno=crash.lineno,
                    message=crash.message,
                )
        elif report.outcome == FLAKY:
            test.outcome = FLAKY

    @pytest.hookimpl(tryfirst=True)
    def pytest_report_teststatus(
        self, report: TestReport
    ) -> tuple[str, str, str] | None:
        # Hook into the terminal reporting of the test status
        if getattr(report, "should_retry", False):
            # Don't log a status for this yet since it's not final
            return "", "", ""
        if report.outcome == FLAKY:
            # outcome category, letter used for regular output, and status used for full output, respectively
            return "flaky", "K", "FLAKY"
        # Otherwise let the default hook implementation decide the status strings
        return None

    def pytest_sessionfinish(self) -> None:
        # Called at the end of the pytest run to log flaky tests to GitHub
        for nodeid, test in self.test_run.tests.items():
            if test.outcome != FLAKY:
                continue

            if self.github and self.test_type:
                try:
                    self.github.create_or_update_flaky_test_issue(
                        self.test_run.root, self.test_type, test
                    )
                except Exception as e:  # noqa
                    # Catch all exceptions to be logged later, don't let this fail the test run
                    self.exceptions[nodeid] = e

    def pytest_terminal_summary(self, terminalreporter: TerminalReporter) -> None:
        # Called at the end of the pytest run to print custom messages to the terminal
        lines = []
        if self.flaky_tests > 0:
            if not self.github:
                lines.append(
                    "Could not report flaky tests because GH_TOKEN was missing"
                )
            if not self.test_type:
                lines.append(
                    f"Could not report flaky tests because {TEST_TYPE_OPTION} was missing"
                )
        for nodeid, e in self.exceptions.items():
            lines.append(
                f"Failed to create or update flaky test issue for {nodeid}: {e}"
            )
        if lines:
            terminalreporter.write_sep("=", f"{self.name} plugin warnings", yellow=True)
            for line in lines:
                terminalreporter.write_line(line)


def pytest_configure(config: Config) -> None:
    # Register our plugin with pytest
    plugin = DeflakePlugin(config)
    config.pluginmanager.register(plugin, name=plugin.name)


def pytest_addoption(parser: Parser):
    # Add a flag so the user can tell us what kind of tests we're running
    # (used by the GitHub class to create issue titles)
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


@dataclass
class TestRun:
    root: Path = field(init=False)
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
            # We prepend a JIRA ticket ID to new issues, so check for then of the title only
            if issue["title"].endswith(self.issue_title(test_type, test)):
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
            if getattr(test, phase).outcome in (FAILED, FLAKY):
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
