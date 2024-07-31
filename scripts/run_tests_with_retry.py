import os
import sys
from subprocess import run

test_type = sys.argv[1].title()
pytest = ["pytest"] + sys.argv[2:]

# Run tests once
if run(pytest, check=False).returncode == 0:
    sys.exit(0)

# Then run the failed tests once more,
# if they fail, the failure was most likely legitimate
print(f"{test_type} tests failed, re-running to detect flakes")
if (ret := run(pytest + ["--last-failed"]).returncode) != 0:
    print(f"{test_type} tests re-run failed")
    sys.exit(ret)

# If they succeed, they are flaky, we should tell the user
print(f"{test_type} tests passed during retry, these are most likely flaky")
if issue := os.environ.get("ISSUE"):
    run(
        [
            "gh",
            "issue",
            "comment",
            issue,
            "--body",
            f"{test_type} test failure detected",
        ]
    )
