from __future__ import annotations

import json
import os
import sys
import tempfile
from operator import itemgetter
from pathlib import Path
from subprocess import run
from textwrap import dedent
from typing import Iterable, cast

import requests

REPO = "snowflakedb/snowflake-cli"
# REPO = "snowflakedb/frank-test"
FLAKY_LABEL = "flaky-test"


github: requests.Session | None
if gh_token := os.getenv("GH_TOKEN"):
    github = requests.Session()
    github.headers.update({"Authorization": f"Bearer {gh_token}"})
else:
    github = None


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        test_type = sys.argv[1].title()

        report_path = Path(tmpdir) / "pytest.json"
        pytest = [
            "pytest",
            "--json-report",
            "--json-report-file",
            f"{report_path}",
            "--json-report-omit",
            "warnings",
            "keywords",
            "environment",
            "streams",
            "log",
        ] + sys.argv[2:]

        # Run tests once
        os.environ["FLAKE"] = "true"
        if run(pytest, check=False).returncode == 0:
            sys.exit(0)
        first_failed_tests = get_failed_tests(report_path)

        # Then run the failed tests once more
        p(f"{test_type} tests failed, re-running to detect flakes")
        del os.environ["FLAKE"]
        returncode = run(pytest + ["--last-failed"]).returncode
        second_failed_tests = get_failed_tests(report_path)

        # Compare reports to see which tests failed then passed
        if flaky_tests := find_flaky_tests(first_failed_tests, second_failed_tests):
            p(f"{test_type} tests passed during retry, these are most likely flaky:")
            for flaky_test in flaky_tests:
                p(flaky_test["nodeid"])
                if github:
                    create_or_update_flaky_test_issue(flaky_test)

        if returncode != 0:
            p(f"{test_type} tests re-run failed")
            sys.exit(returncode)


def get_failed_tests(report: Path) -> dict[str, dict]:
    with report.open() as f:
        report_data = json.load(f)
    tests_by_node_id = {}
    for test in report_data["tests"]:
        if test["outcome"] == "failed":
            tests_by_node_id[test["nodeid"]] = test
    return tests_by_node_id


def find_flaky_tests(
    first_failed_tests: dict[str, dict], second_failed_tests: dict[str, dict]
) -> Iterable[dict]:
    return sorted(
        (
            test
            for nodeid, test in first_failed_tests.items()
            if nodeid not in second_failed_tests
        ),
        key=itemgetter("nodeid"),
    )


def create_or_update_flaky_test_issue(test: dict) -> None:
    if existing_issue := get_issue(test):
        ensure_issue_open(existing_issue)
        comment_on_issue(existing_issue)
    else:
        create_issue(test)


def get_issue(test: dict) -> dict[str, dict] | None:
    issues = get(f"repos/{REPO}/issues", labels=FLAKY_LABEL, state="all")
    for issue in issues:
        if issue["title"] == flaky_test_title(test):
            return issue
    return None


def ensure_issue_open(issue: dict) -> None:
    if issue["state"] != "open":
        number = issue["number"]
        patch(f"repos/{REPO}/issues/{number}", state="open")


def comment_on_issue(issue: dict) -> dict:
    body = "+1"
    number = issue["number"]
    return cast(dict, post(f"repos/{REPO}/issues/{number}/comments", body=body))


def create_issue(test: dict) -> dict:
    title = flaky_test_title(test)
    body = dedent(
        f"""\
        Fill in body later
        """
    )
    return cast(
        dict,
        post(
            f"repos/{REPO}/issues",
            title=title,
            body=body,
            labels=[FLAKY_LABEL],
        ),
    )


def flaky_test_title(test: dict) -> str:
    return f"Flaky test: {test['nodeid']}"


def get(path: str, **params) -> dict | list:
    return api("get", path, params=params)


def post(path: str, **data) -> dict | list:
    return api("post", path, json=data)


def patch(path: str, **data) -> dict | list:
    return api("patch", path, json=data)


def api(method, path, *args, **kwargs) -> dict | list:
    assert github, "Github API must be configured"
    url = f"https://api.github.com/{path}"
    resp = github.request(method, url, *args, **kwargs)
    resp.raise_for_status()
    return resp.json()


def p(*s):
    print(*s, file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
