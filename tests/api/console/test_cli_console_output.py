from __future__ import annotations

from typing import Generator

import pytest
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.console.console import CliConsole


@pytest.fixture(name="cli_console")
def make_cli_console() -> Generator[CliConsole, None, None]:
    console = CliConsole(cli_context=cli_context)
    yield console


def assert_output_matches(expected: str, capsys):
    out, _ = capsys.readouterr()
    assert out == expected


def test_only_phase_no_indent(cli_console, capsys):
    cli_console.phase("42")
    assert_output_matches("42\n", capsys)


def test_only_step_no_indent(cli_console, capsys):
    cli_console.step("73")
    assert_output_matches("  73\n", capsys)


def test_step_indented_in_phase(cli_console, capsys):
    cli_console.phase("42")
    cli_console.step("73")
    assert_output_matches("42\n  73\n", capsys)


def test_multi_step_indented(cli_console, capsys):
    cli_console.phase("42")
    cli_console.step("73.1")
    cli_console.step("73.2")
    assert_output_matches("42\n  73.1\n  73.2\n", capsys)


def test_phase_after_step_not_indented(cli_console, capsys):
    cli_console.phase("42")
    cli_console.step("73")
    cli_console.phase("42")
    assert_output_matches("42\n  73\n42\n", capsys)
