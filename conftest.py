import sys
from unittest import mock

import pytest
from prompt_toolkit.output import DummyOutput


@pytest.fixture(name="win32_dummy_console", autouse=True)
def make_win32_dummy_console():
    if sys.platform == "win32":
        to_patch = "prompt_toolkit.output.deautls.create_output"
        with mock.patch(to_patch, return_value=DummyOutput()):
            yield
    else:
        yield
