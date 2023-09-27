import subprocess
from timeit import default_timer as timer

import pytest

SAMPLE_AMOUNT = 20
EXECUTION_TIME_THRESHOLD = 1.3


@pytest.mark.performance
def test_snow_help_performance():
    results = []
    for _ in range(SAMPLE_AMOUNT):
        start = timer()
        subprocess.run(["snow", "--help"], stdout=subprocess.DEVNULL)
        end = timer()
        results.append(end - start)

    results.sort()
    assert results[int(SAMPLE_AMOUNT * 0.9)] <= EXECUTION_TIME_THRESHOLD
