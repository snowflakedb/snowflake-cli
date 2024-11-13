# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from textwrap import dedent
from typing import Generator

import pytest
from snowflake.cli.api.console.console import (
    CliConsole,
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


def test_phase_nesting(cli_console, capsys):
    with cli_console.phase("Enter 1"):
        with cli_console.phase("Enter 2"):
            with cli_console.phase("Enter 3"):
                pass

    expected_output = dedent(
        f"""\
    Enter 1
      Enter 2
        Enter 3
    """
    )

    assert_output_matches(expected_output, capsys)


def test_phase_is_cleaned_up_on_exception(cli_console):
    with pytest.raises(RuntimeError):
        with cli_console.phase("Enter 1"):
            raise RuntimeError("Phase failed")

    # If the phase is cleaned up correctly, this will not raise any exception
    with cli_console.phase("Enter 2") as step:
        pass


def test_phase_inside_indented(cli_console, capsys):
    cli_console.step("Outside of Indent")
    with cli_console.indented():
        cli_console.step("Step In Indent")
        with cli_console.phase("Phase In Indent"):
            cli_console.step("Step In Indent + Phase")

    expected_output = dedent(
        f"""\
        Outside of Indent
          Step In Indent
          Phase In Indent
            Step In Indent + Phase
        """
    )

    assert_output_matches(expected_output, capsys)


def test_step_inside_indented(cli_console, capsys):
    cli_console.step("Outside of Indent")
    with cli_console.indented():
        cli_console.step("Operation")

    expected_output = dedent(
        f"""\
        Outside of Indent
          Operation
        """
    )

    assert_output_matches(expected_output, capsys)


def test_indented(cli_console, capsys):
    with cli_console.phase("42"):
        cli_console.step("73")
        cli_console.message("Not indented message")
        cli_console.warning("Not indented warning")
        with cli_console.indented():
            cli_console.message("Indented message")
            cli_console.warning("Indented warning")
            with cli_console.indented():
                cli_console.message("Double indented message")
                cli_console.warning("Double indented warning")
            cli_console.message("Message with single indentation again")
            cli_console.warning("Warning with single indentation again")
        cli_console.message("No longer indented message")
        cli_console.warning("No longer indented warning")
    cli_console.warning("OPS")

    expected_output = dedent(
        f"""\
    42
      73
      Not indented message
      Not indented warning
        Indented message
        Indented warning
          Double indented message
          Double indented warning
        Message with single indentation again
        Warning with single indentation again
      No longer indented message
      No longer indented warning
    OPS
    """
    )

    assert_output_matches(expected_output, capsys)


def test_indented_cleans_up_on_exception(cli_console, capsys):
    with pytest.raises(RuntimeError):
        with cli_console.indented():
            raise RuntimeError("Failure")

    # If the phase is cleaned up correctly, this will not raise any exception
    cli_console.message("Not indented message")

    assert_output_matches("Not indented message\n", capsys)
