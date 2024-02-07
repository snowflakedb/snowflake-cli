from __future__ import annotations

from typing import Generator

import pytest
from snowflake.cli.api.console.console import (
    CliConsole,
    CliConsoleNestingProhibitedError,
)


@pytest.fixture(name="cli_console")
def make_cli_console() -> Generator[CliConsole, None, None]:
    console = CliConsole()
    yield console


def assert_output_matches(expected: str, capsys):
    out, _ = capsys.readouterr()
    assert out == expected


def test_phase_alone_produces_no_output(cli_console, capsys):
    cli_console.phase("42")
    assert_output_matches("", capsys)


def test_only_step_no_indent(cli_console, capsys):
    cli_console.step("73")
    assert_output_matches("73\n", capsys)


def test_step_indented_in_phase(cli_console, capsys):
    with cli_console.phase("42"):
        cli_console.step("73")
    assert_output_matches("42\n  73\n", capsys)


def test_multi_step_indented(cli_console, capsys):
    with cli_console.phase("42"):
        cli_console.step("73.1")
        cli_console.step("73.2")
    assert_output_matches("42\n  73.1\n  73.2\n", capsys)


def test_phase_after_step_not_indented(cli_console, capsys):
    with cli_console.phase("42"):
        cli_console.step("73")
    cli_console.step("42")
    assert_output_matches("42\n  73\n42\n", capsys)


def test_error_messages(cli_console, capsys):
    with cli_console.phase("42"):
        cli_console.step("73")
        cli_console.warning("ops")
    cli_console.warning("OPS")

    assert_output_matches("42\n  73\n  ops\nOPS\n", capsys)


def test_phase_nesting_not_allowed(cli_console):
    with cli_console.phase("Enter 1"):
        with pytest.raises(CliConsoleNestingProhibitedError):
            with cli_console.phase("Enter 2"):
                pass
