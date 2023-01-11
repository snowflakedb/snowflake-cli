"""These tests verify that the CLI runs work as expected."""
from __future__ import annotations


def test_help_option(runner):
    result = runner.invoke(["--help"])
    assert result.exit_code == 0
