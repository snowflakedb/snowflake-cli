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

from unittest.mock import patch

import pytest
from snowflake.cli.api.constants import IS_WINDOWS

COMMAND = "detect-encoding"


def test_detect_encoding_clean_system(runner, monkeypatch):
    """On a well-configured UTF-8 system the command reports no issues."""
    monkeypatch.setattr("sys.getfilesystemencoding", lambda: "utf-8")
    monkeypatch.setattr("sys.getdefaultencoding", lambda: "utf-8")
    monkeypatch.setattr("locale.getpreferredencoding", lambda: "utf-8")
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_STDOUT", raising=False)
    # Pin this so the assertion below doesn't depend on the console this
    # test happens to run in (e.g. a real Windows box with a non-UTF-8
    # console codepage).
    monkeypatch.setattr(
        "snowflake.cli.api.encoding_diagnostics._console_needs_utf8_configuration",
        lambda: False,
    )

    result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output
    assert "No encoding issues" in result.output
    assert "properly configured" in result.output


def test_detect_encoding_mismatch(runner, monkeypatch):
    """When the platform encodings differ the command reports each encoding and
    includes actionable remediation steps."""
    monkeypatch.setattr("sys.getfilesystemencoding", lambda: "cp1252")
    monkeypatch.setattr("sys.getdefaultencoding", lambda: "utf-8")
    monkeypatch.setattr("locale.getpreferredencoding", lambda: "utf-16")
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_STDOUT", raising=False)

    result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output
    assert "Encoding mismatch detected" in result.output
    assert "cp1252" in result.output
    assert "utf-8" in result.output
    assert "utf-16" in result.output
    assert "PYTHONUTF8" in result.output


def test_detect_encoding_non_utf8(runner, monkeypatch):
    """A single consistent non-UTF-8 encoding triggers the platform encoding
    report with remediation advice."""
    monkeypatch.setattr("sys.getfilesystemencoding", lambda: "cp1252")
    monkeypatch.setattr("sys.getdefaultencoding", lambda: "cp1252")
    monkeypatch.setattr("locale.getpreferredencoding", lambda: "cp1252")
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_STDOUT", raising=False)

    result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output
    assert "cp1252" in result.output
    assert "PYTHONUTF8" in result.output
    assert "No encoding issues" not in result.output


def test_detect_encoding_both_configured(runner, monkeypatch):
    """When both CLI encodings are explicitly configured the command reports no
    issues even if the underlying platform encodings are inconsistent."""
    monkeypatch.setattr("sys.getfilesystemencoding", lambda: "cp1252")
    monkeypatch.setattr("sys.getdefaultencoding", lambda: "utf-8")
    monkeypatch.setattr("locale.getpreferredencoding", lambda: "utf-16")
    monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", "utf-8")
    monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", "utf-8")
    monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_STDOUT", "utf-8")
    monkeypatch.setattr(
        "snowflake.cli.api.encoding_diagnostics._console_needs_utf8_configuration",
        lambda: False,
    )

    result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output
    assert "No encoding issues" in result.output
    assert "properly configured" in result.output


def test_detect_encoding_does_not_require_connection(runner):
    """The command must be runnable without a Snowflake connection."""
    with patch(
        "snowflake.cli._plugins.helpers.commands.get_encoding_diagnostics",
        return_value="No encoding issues - your system is properly configured.",
    ):
        result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output


# ---------------------------------------------------------------------------
# _console_output_codepage / _console_needs_utf8_configuration
# (see snowflake.cli.api.windows_console_encoding)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(IS_WINDOWS, reason="exercises the real non-Windows early return")
def test_console_output_codepage_none_on_non_windows():
    from snowflake.cli.api.windows_console_encoding import _console_output_codepage

    assert _console_output_codepage() is None


@pytest.mark.skipif(not IS_WINDOWS, reason="exercises the real Win32 ctypes wrapper")
def test_console_output_codepage_matches_win32_api_on_real_windows():
    """Runs the real GetConsoleOutputCP call (no IS_WINDOWS/ctypes stubbing) so
    CI on a Windows runner actually exercises the ctypes wrapper.

    Does not assert on _console_needs_utf8_configuration(): CI's stdout is
    usually not a real TTY, so that predicate is False regardless of whether
    ctypes works.
    """
    from ctypes import windll

    from snowflake.cli.api.windows_console_encoding import _console_output_codepage

    result = _console_output_codepage()
    assert result is None or (isinstance(result, int) and result > 0)
    assert result == (windll.kernel32.GetConsoleOutputCP() or None)


@pytest.mark.skipif(
    not IS_WINDOWS, reason="exercises the real Windows ctypes import path"
)
def test_console_output_codepage_none_when_ctypes_call_raises(monkeypatch):
    from snowflake.cli.api.windows_console_encoding import _console_output_codepage

    def _raise(*_args, **_kwargs):
        raise OSError("boom")

    monkeypatch.setattr("ctypes.windll.kernel32.GetConsoleOutputCP", _raise)
    assert _console_output_codepage() is None


@pytest.mark.skipif(IS_WINDOWS, reason="exercises the real non-Windows early return")
def test_console_needs_utf8_configuration_false_on_non_windows():
    from snowflake.cli.api.windows_console_encoding import (
        _console_needs_utf8_configuration,
    )

    assert _console_needs_utf8_configuration() is False


@pytest.mark.skipif(not IS_WINDOWS, reason="exercises the real Windows branch")
@pytest.mark.parametrize(
    "is_tty,codepage,expected",
    [
        (False, 437, False),  # not attached to a real console
        (True, 65001, False),  # already UTF-8
        (True, 437, True),  # misconfigured
        (True, None, False),  # codepage unavailable
    ],
)
def test_console_needs_utf8_configuration_on_windows(
    monkeypatch, is_tty, codepage, expected
):
    from snowflake.cli.api.windows_console_encoding import (
        _console_needs_utf8_configuration,
    )

    monkeypatch.setattr("sys.stdout.isatty", lambda: is_tty)
    monkeypatch.setattr(
        "snowflake.cli.api.windows_console_encoding._console_output_codepage",
        lambda: codepage,
    )

    assert _console_needs_utf8_configuration() is expected


_CHCP_MARKER = "chcp.com 65001"
_POWERSHELL_DOCS_MARKER = "configure-cli#additional-step-for-windows-powershell-5x"


def test_detect_encoding_console_section_shown_on_windows_with_mismatch(
    runner, monkeypatch
):
    """When the console is misconfigured, the diagnostics mention both the
    chcp.com fix and the PowerShell docs link."""
    monkeypatch.setattr("sys.getfilesystemencoding", lambda: "utf-8")
    monkeypatch.setattr("sys.getdefaultencoding", lambda: "utf-8")
    monkeypatch.setattr("locale.getpreferredencoding", lambda: "cp932")
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_STDOUT", raising=False)

    with patch(
        "snowflake.cli.api.encoding_diagnostics._console_needs_utf8_configuration",
        return_value=True,
    ):
        result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output
    assert _CHCP_MARKER in result.output
    assert _POWERSHELL_DOCS_MARKER in result.output
    assert "Additionally, your console is not configured" in result.output


def test_detect_encoding_console_section_shown_when_cli_encoding_configured(
    runner, monkeypatch
):
    """Show the console note even when all CLI encodings are explicitly configured."""
    monkeypatch.setattr("sys.getfilesystemencoding", lambda: "utf-8")
    monkeypatch.setattr("sys.getdefaultencoding", lambda: "utf-8")
    monkeypatch.setattr("locale.getpreferredencoding", lambda: "cp932")
    monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", "utf-8")
    monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", "utf-8")
    monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_STDOUT", "utf-8")

    with patch(
        "snowflake.cli.api.encoding_diagnostics._console_needs_utf8_configuration",
        return_value=True,
    ):
        result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output
    assert _CHCP_MARKER in result.output
    assert _POWERSHELL_DOCS_MARKER in result.output


def test_detect_encoding_no_console_section_when_configured(runner, monkeypatch):
    """Console section is suppressed once the console is already UTF-8."""
    monkeypatch.setattr("sys.getfilesystemencoding", lambda: "utf-8")
    monkeypatch.setattr("sys.getdefaultencoding", lambda: "utf-8")
    monkeypatch.setattr("locale.getpreferredencoding", lambda: "cp932")
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_STDOUT", raising=False)

    with patch(
        "snowflake.cli.api.encoding_diagnostics._console_needs_utf8_configuration",
        return_value=False,
    ):
        result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output
    assert _CHCP_MARKER not in result.output
    assert _POWERSHELL_DOCS_MARKER not in result.output


def test_detect_encoding_diagnostics_headline_when_platform_clean_but_console_misconfigured(
    runner, monkeypatch
):
    """When the platform encodings already agree on utf-8 (no mismatch, no CLI
    encoding config needed) but the console isn't UTF-8, the diagnostics use
    the same "correctly configured" headline as the CLI-encodings-set case —
    there should be exactly one such headline, not two different ones."""
    monkeypatch.setattr("sys.getfilesystemencoding", lambda: "utf-8")
    monkeypatch.setattr("sys.getdefaultencoding", lambda: "utf-8")
    monkeypatch.setattr("locale.getpreferredencoding", lambda: "utf-8")
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_STDOUT", raising=False)

    with patch(
        "snowflake.cli.api.encoding_diagnostics._console_needs_utf8_configuration",
        return_value=True,
    ):
        result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output
    assert "CLI encoding is correctly configured." in result.output
    assert "However, your console is not configured" in result.output
    assert _CHCP_MARKER in result.output
