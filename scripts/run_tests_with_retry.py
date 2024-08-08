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
            "pytest",
            "--json-report",
            "--json-report-omit",
            "warnings",
            "keywords",
            "environment",
            "collectors",
            "streams",
            "log",
        ] + pytest_args

        # Run tests once
        report_path = Path(tmpdir) / "pytest1.json"
        pytest = base_pytest + ["--json-report-file", str(report_path)]
        if run(pytest, check=False).returncode == 0:
            sys.exit(0)
        first_failed_tests = get_failed_tests(report_path)

        # Then run the failed tests once more
        p(f"{test_type} tests failed, re-running to detect flakes")
        report_path = Path(tmpdir) / "pytest2.json"
        pytest = base_pytest + ["--json-report-file", str(report_path), "--last-failed"]
        final_exit_code = run(pytest, check=False).returncode

        try:
            # Compare reports to see which tests failed then passed
            second_failed_tests = get_failed_tests(report_path)
            if flaky_tests := find_flaky_tests(first_failed_tests, second_failed_tests):
                p(
                    f"{test_type} tests passed during retry, these are most likely flaky:"
                )
                for flaky_test in flaky_tests:
                    nodeid = flaky_test["nodeid"]
                    p(nodeid)
                    if github:
                        with report_path.open() as f:
                            report_data = json.load(f)
                            root = Path(report_data["root"])
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


EXAMPLE = {
    "created": 1724090139.107965,
    "duration": 0.1522068977355957,
    "exitcode": 1,
    "root": "/Users/fcampbell/Software/snowflakedb/snowflake-cli",
    "environment": {},
    "summary": {"failed": 4, "passed": 1, "error": 2, "total": 7, "collected": 7},
    "tests": [
        {
            "nodeid": "tests_integration/test_test.py::test_fail_parametrized[1]",
            "lineno": 35,
            "outcome": "failed",
            "setup": {"duration": 0.062450499972328544, "outcome": "passed"},
            "call": {
                "duration": 0.00045116699766367674,
                "outcome": "failed",
                "crash": {
                    "path": "/Users/fcampbell/Software/snowflakedb/snowflake-cli/tests_integration/test_test.py",
                    "lineno": 39,
                    "message": "AssertionError: assert 1 == 'foobar'",
                },
                "traceback": [
                    {
                        "path": "tests_integration/test_test.py",
                        "lineno": 39,
                        "message": "AssertionError",
                    }
                ],
                "longrepr": 'somevar = 1\n\n    @pytest.mark.integration\n    @pytest.mark.parametrize("somevar", [1, True, "3"])\n    def test_fail_parametrized(somevar):\n>       assert somevar == "foobar"\nE       AssertionError: assert 1 == \'foobar\'\n\ntests_integration/test_test.py:39: AssertionError',
            },
            "teardown": {"duration": 0.0009553750278428197, "outcome": "passed"},
        },
        {
            "nodeid": "tests_integration/test_test.py::test_successful",
            "lineno": 25,
            "outcome": "passed",
            "setup": {"duration": 0.0029910000157542527, "outcome": "passed"},
            "call": {"duration": 0.00015962502220645547, "outcome": "passed"},
            "teardown": {"duration": 0.0008229169761762023, "outcome": "passed"},
        },
        {
            "nodeid": "tests_integration/test_test.py::test_fail",
            "lineno": 30,
            "outcome": "failed",
            "setup": {"duration": 0.0027732919552363455, "outcome": "passed"},
            "call": {
                "duration": 0.0003886250196956098,
                "outcome": "failed",
                "crash": {
                    "path": "/Users/fcampbell/Software/snowflakedb/snowflake-cli/tests_integration/test_test.py",
                    "lineno": 33,
                    "message": "assert 1 == 2",
                },
                "traceback": [
                    {
                        "path": "tests_integration/test_test.py",
                        "lineno": 33,
                        "message": "AssertionError",
                    }
                ],
                "longrepr": "@pytest.mark.integration\n    def test_fail():\n>       assert 1 == 2\nE       assert 1 == 2\n\ntests_integration/test_test.py:33: AssertionError",
            },
            "teardown": {"duration": 0.000846582988742739, "outcome": "passed"},
        },
        {
            "nodeid": "tests_integration/test_test.py::test_fail_parametrized[True]",
            "lineno": 35,
            "outcome": "failed",
            "setup": {"duration": 0.00286454102024436, "outcome": "passed"},
            "call": {
                "duration": 0.00038183294236660004,
                "outcome": "failed",
                "crash": {
                    "path": "/Users/fcampbell/Software/snowflakedb/snowflake-cli/tests_integration/test_test.py",
                    "lineno": 39,
                    "message": "AssertionError: assert True == 'foobar'",
                },
                "traceback": [
                    {
                        "path": "tests_integration/test_test.py",
                        "lineno": 39,
                        "message": "AssertionError",
                    }
                ],
                "longrepr": 'somevar = True\n\n    @pytest.mark.integration\n    @pytest.mark.parametrize("somevar", [1, True, "3"])\n    def test_fail_parametrized(somevar):\n>       assert somevar == "foobar"\nE       AssertionError: assert True == \'foobar\'\n\ntests_integration/test_test.py:39: AssertionError',
            },
            "teardown": {"duration": 0.0008507499587722123, "outcome": "passed"},
        },
        {
            "nodeid": "tests_integration/test_test.py::test_teardown_failure",
            "lineno": 57,
            "outcome": "error",
            "setup": {"duration": 0.0037783330189995468, "outcome": "passed"},
            "call": {"duration": 0.00018399994587525725, "outcome": "passed"},
            "teardown": {
                "duration": 0.0009889589855447412,
                "outcome": "failed",
                "crash": {
                    "path": "/Users/fcampbell/Software/snowflakedb/snowflake-cli/tests_integration/test_test.py",
                    "lineno": 55,
                    "message": "ValueError: teardown failure",
                },
                "traceback": [
                    {
                        "path": "tests_integration/test_test.py",
                        "lineno": 55,
                        "message": "ValueError",
                    }
                ],
                "longrepr": '@pytest.fixture\n    def fail_teardown():\n        yield\n>       raise ValueError("teardown failure")\nE       ValueError: teardown failure\n\ntests_integration/test_test.py:55: ValueError',
            },
        },
        {
            "nodeid": "tests_integration/test_test.py::test_fail_parametrized[3]",
            "lineno": 35,
            "outcome": "failed",
            "setup": {"duration": 0.0027936670230701566, "outcome": "passed"},
            "call": {
                "duration": 0.00043699995148926973,
                "outcome": "failed",
                "crash": {
                    "path": "/Users/fcampbell/Software/snowflakedb/snowflake-cli/tests_integration/test_test.py",
                    "lineno": 39,
                    "message": "AssertionError: assert '3' == 'foobar'\n  \n  - foobar\n  + 3",
                },
                "traceback": [
                    {
                        "path": "tests_integration/test_test.py",
                        "lineno": 39,
                        "message": "AssertionError",
                    }
                ],
                "longrepr": "somevar = '3'\n\n    @pytest.mark.integration\n    @pytest.mark.parametrize(\"somevar\", [1, True, \"3\"])\n    def test_fail_parametrized(somevar):\n>       assert somevar == \"foobar\"\nE       AssertionError: assert '3' == 'foobar'\nE         \nE         - foobar\nE         + 3\n\ntests_integration/test_test.py:39: AssertionError",
            },
            "teardown": {"duration": 0.0010245830053463578, "outcome": "passed"},
        },
        {
            "nodeid": "tests_integration/test_test.py::test_setup_failure",
            "lineno": 46,
            "outcome": "error",
            "setup": {
                "duration": 0.002809500030707568,
                "outcome": "failed",
                "crash": {
                    "path": "/Users/fcampbell/Software/snowflakedb/snowflake-cli/tests_integration/test_test.py",
                    "lineno": 44,
                    "message": "ValueError: setup failure",
                },
                "traceback": [
                    {
                        "path": "tests_integration/test_test.py",
                        "lineno": 44,
                        "message": "ValueError",
                    }
                ],
                "longrepr": '@pytest.fixture\n    def fail_setup():\n>       raise ValueError("setup failure")\nE       ValueError: setup failure\n\ntests_integration/test_test.py:44: ValueError',
            },
            "teardown": {"duration": 0.0010127919958904386, "outcome": "passed"},
        },
    ],
}

if __name__ == "__main__":
    main(
        test_type=sys.argv[1].title(),
        pytest_args=sys.argv[2:],
    )
