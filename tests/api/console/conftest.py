from __future__ import annotations

from typing import Generator

import pytest
from snowflake.cli.api.console.context import CliConsoleContext


@pytest.fixture(name="cli_console_ctx")
def make_cli_console_ctx() -> Generator[CliConsoleContext, None, None]:
    ctx = CliConsoleContext()
    yield ctx
    ctx.reset()
