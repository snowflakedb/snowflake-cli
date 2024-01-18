from __future__ import annotations

from typing import Generator

import pytest
from snowflake.cli.api.commands.decorators import global_options
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.console.context import CliConsoleContext
from snowflake.cli.app.cli_app import app


@pytest.fixture(name="cli_console_ctx")
def make_cli_console_ctx() -> Generator[CliConsoleContext, None, None]:
    ctx = CliConsoleContext()
    yield ctx
    ctx.reset()


@pytest.fixture(name="faker_app")
def make_faker_app():
    @app.command("Faker")
    @global_options
    def faker_app(**options):
        """Faker app"""
        cli_console.phase("Faker. Phase UNO.")
        cli_console.step("Faker. Teeny Tiny step: UNO UNO")

    yield
