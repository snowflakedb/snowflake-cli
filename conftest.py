import sys
from unittest import mock

import pytest
from prompt_toolkit.output import DummyOutput
from rich.panel import Panel
from snowflake.cli.api.config import (
    CLI_SECTION,
    IGNORE_NEW_VERSION_WARNING_KEY,
    get_env_variable_name,
)

IGNORE_NEW_VERSION_WARNING_ENV = get_env_variable_name(
    CLI_SECTION, key=IGNORE_NEW_VERSION_WARNING_KEY
)


@pytest.fixture(autouse=True)
def ignore_new_version_warning(monkeypatch):
    """`snow --help` renders the new-version notice as the Typer epilog, so every
    snapshot of help output would otherwise depend on what is published to brew
    and PyPI at the moment the suite runs. Pinning the notice off also keeps the
    suite from calling out to those repositories.

    tests/app/test_version_check.py opts back out, since it asserts on the notice.
    """
    monkeypatch.setenv(IGNORE_NEW_VERSION_WARNING_ENV, "true")


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
