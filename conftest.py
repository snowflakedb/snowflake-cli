import sys
from unittest import mock

import pytest
from prompt_toolkit.output import DummyOutput
from rich.panel import Panel


@pytest.fixture(name="win32_dummy_console", autouse=True)
def make_win32_dummy_console():
    """Windows in CI/CD does not provide full terminal.
    We need to patch default output with DummyOutput()
    DummyOutput does not provide detection of the capabilities
    and we need to force rich Panel not to play safe.
    """
    if sys.platform == "win32":
        to_patch = "prompt_toolkit.output.defaults.create_output"
        with mock.patch(to_patch, return_value=DummyOutput()):
            with mock.patch("snowflake.cli.api.console.console.Panel") as mock_panel:
                mock_panel.side_effect = lambda *args, **kwargs: Panel(
                    *args,
                    **kwargs,
                    safe_box=False,
                )
                yield
    else:
        yield
