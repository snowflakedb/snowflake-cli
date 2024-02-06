import pytest
import sys

from tests.testing_utils.fixtures import *


@pytest.mark.loaded_modules
def test_loaded_modules(runner):
    should_not_load = {"git"}

    runner.invoke(["sql", "-q", "select 1"])

    loaded_modules = sys.modules.keys()
    assert loaded_modules.isdisjoint(should_not_load)
