import sys

import pytest


@pytest.mark.loaded_modules
def test_loaded_modules(runner):
    should_not_load = {"git"}

    runner.invoke(["sql", "-q", "select 1"])

    loaded_modules = sys.modules.keys()
    assert loaded_modules.isdisjoint(should_not_load)
