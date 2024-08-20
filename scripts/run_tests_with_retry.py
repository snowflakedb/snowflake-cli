from __future__ import annotations

import json
import os
import sys
import tempfile
from operator import itemgetter
from pathlib import Path
from subprocess import PIPE, run
from typing import Generator, Iterable, cast

import requests

APP_REPO = "snowflakedb/snowflake-cli"
ISSUE_REPO = APP_REPO
# ISSUE_REPO = "snowflakedb/frank-test"
FLAKY_LABEL = "flaky-test"
TEST_STEPS = ["setup", "call", "teardown"]

SHA = run(["git", "rev-parse", "HEAD"], text=True, stdout=PIPE).stdout.strip()


def p(*s):
    print(*s, file=sys.stderr, flush=True)


github: requests.Session | None
if gh_token := os.getenv("GH_TOKEN"):
    github = requests.Session()
    github.headers.update({"Authorization": f"Bearer {gh_token}"})
else:
    github = None
    p("GH_TOKEN not provided, issue reporting disabled.")


def main(test_type: str, pytest_args: list[str]):
    with tempfile.TemporaryDirectory() as tmpdir:
        base_pytest = [
            "python",
            "-m",
            "pytest",
            "-p",
            "tests_common.pytest_json_report",
        ] + pytest_args

        # Run tests once
        report_path = Path(tmpdir) / "pytest1.json"
        pytest = base_pytest + ["--json-file", str(report_path)]
        flake_env = os.environ | dict(FLAKE="true")
        if run(pytest, check=False, env=flake_env).returncode == 0:
            sys.exit(0)
        root, first_failed_tests = get_failed_tests(report_path)

        # Then run the failed tests once more
        p(f"{test_type} tests failed, re-running to detect flakes")
        report_path = Path(tmpdir) / "pytest2.json"
        pytest = base_pytest + ["--json-file", str(report_path), "--last-failed"]
        final_exit_code = run(pytest, check=False).returncode

        try:
            # Compare reports to see which tests failed then passed
            _, second_failed_tests = get_failed_tests(report_path)
            if flaky_tests := find_flaky_tests(first_failed_tests, second_failed_tests):
                p(
                    f"{test_type} tests passed during retry, these are most likely flaky:"
                )
                for flaky_test in flaky_tests:
                    nodeid = flaky_test["nodeid"]
                    p(nodeid)
                    if github:
                        try:
                            create_or_update_flaky_test_issue(
                                root, test_type, flaky_test
                            )
                        except Exception as e_inner:
                            p(f"Failed to create or update flaky test issue: {e_inner}")
        except Exception as e_outer:
            p(f"Flaky test reporting failed, ignoring to not let tests fail: {e_outer}")

        if final_exit_code != 0:
            p(f"{test_type} tests re-run failed")
            sys.exit(final_exit_code)


def get_failed_tests(report: Path) -> tuple[Path, dict[str, dict]]:
    with report.open() as f:
        report_data = json.load(f)
    root = Path(report_data["root"])
    tests = {
        nodeid: test
        for nodeid, test in report_data["tests"].items()
        if test["outcome"] == "failed"
    }
    return root, tests


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


def create_or_update_flaky_test_issue(root: Path, test_type: str, test: dict) -> None:
    if existing_issue := get_issue(test_type, test):
        ensure_issue_open(existing_issue)
        update_issue_body(existing_issue, root, test)
        comment_on_issue(existing_issue, test)
    else:
        create_issue(root, test_type, test)


def get_issue(test_type: str, test: dict) -> dict[str, dict] | None:
    issues = get(f"repos/{ISSUE_REPO}/issues", labels=FLAKY_LABEL, state="all")
    for issue in issues:
        if issue["title"] == issue_title(test_type, test):
            return issue
    return None


def ensure_issue_open(issue: dict) -> None:
    if issue["state"] != "open":
        number = issue["number"]
        patch(f"repos/{ISSUE_REPO}/issues/{number}", state="open")


def update_issue_body(issue: dict, root: Path, test: dict) -> None:
    number = issue["number"]
    body = issue_body(root, test)
    patch(f"repos/{ISSUE_REPO}/issues/{number}", body=body)


def comment_on_issue(issue: dict, test: dict) -> dict:
    body = "+1"
    for step in TEST_STEPS:
        if step in test and test[step]["outcome"] == "failed":
            body += f"\n```python\n{test[step]['crash']['message']}\n```\n"
            break

    number = issue["number"]
    return cast(dict, post(f"repos/{ISSUE_REPO}/issues/{number}/comments", body=body))


def create_issue(root: Path, test_type: str, test: dict) -> dict:
    title = issue_title(test_type, test)
    body = issue_body(root, test)
    return cast(
        dict,
        post(
            f"repos/{ISSUE_REPO}/issues",
            title=title,
            body=body,
            labels=[FLAKY_LABEL],
        ),
    )


def issue_title(test_type: str, test: dict) -> str:
    test_name = nodeid_function_name(test["nodeid"])
    return f"Flaky {test_type.lower()} test: `{test_name}`"


def issue_body(root: Path, test: dict) -> str:
    return "\n".join(
        [f"Nodeid: `{test['nodeid']}`"]
        + [
            line
            for step in TEST_STEPS
            for line in step_lines(root, step, test.get(step))
        ]
    )


def step_lines(
    root: Path, step: str, step_info: dict | None
) -> Generator[str, None, None]:
    if step_info is None:
        return

    if step_info["outcome"] == "passed":
        yield f"# 🟢 {step.title()} passed"
        return

    yield f"# 🔴 {step.title()} failed"
    yield f"```python\n{step_info['longrepr']}\n```"

    crash_info = step_info["crash"]
    path = Path(crash_info["path"]).relative_to(root).as_posix()
    lineno = crash_info["lineno"]
    url = f"https://github.com/{APP_REPO}/blob/{SHA}/{path}#L{lineno}"
    yield url


def nodeid_function_name(nodeid: str) -> str:
    return nodeid.split("::", 1)[-1]


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


if __name__ == "__main__":
    main(
        test_type=sys.argv[1].title(),
        pytest_args=sys.argv[2:],
    )
