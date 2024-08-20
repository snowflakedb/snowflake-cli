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

import sys
from subprocess import run

test_type = sys.argv[1].title()
pytest = ["pytest"] + sys.argv[2:]


def p(*s):
    print(*s, file=sys.stderr, flush=True)


# Run tests once
if run(pytest, check=False).returncode == 0:
    sys.exit(0)

# Then run the failed tests once more,
# if they fail, the failure was most likely legitimate
p(f"{test_type} tests failed, re-running to detect flakes")
if (ret := run(pytest + ["--last-failed"]).returncode) != 0:
    p(f"{test_type} tests re-run failed")
    sys.exit(ret)

# If they succeed, they are flaky, we should tell the user
p(f"{test_type} tests passed during retry, these are most likely flaky")
