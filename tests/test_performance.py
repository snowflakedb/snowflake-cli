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
import os
import subprocess
from timeit import default_timer as timer

import pytest

SAMPLE_AMOUNT = 20
EXECUTION_TIME_THRESHOLD = 3.1  # seconds


@pytest.mark.performance
def test_snow_help_performance():
    results = []
    for _ in range(SAMPLE_AMOUNT):
        start = timer()
        subprocess.run(
            ["snow", "--help"],
            stdout=subprocess.DEVNULL,
            env={"SNOWFLAKE_FEATURE_ENABLE_SNOWFLAKE_PROJECTS": "False", **os.environ},
        )
        end = timer()
        results.append(end - start)

    results.sort()
    assert (
        results[int(SAMPLE_AMOUNT * 0.9)] <= EXECUTION_TIME_THRESHOLD
    ), f"90th percentile is too high: {results}"
