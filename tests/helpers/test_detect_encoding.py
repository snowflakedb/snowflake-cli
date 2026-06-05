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

COMMAND = "detect-encoding"


def test_detect_encoding_clean_system(runner, monkeypatch):
    """On a well-configured UTF-8 system the command reports no issues."""
    monkeypatch.setattr("sys.getfilesystemencoding", lambda: "utf-8")
    monkeypatch.setattr("sys.getdefaultencoding", lambda: "utf-8")
    monkeypatch.setattr("locale.getpreferredencoding", lambda: "utf-8")
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", raising=False)
    monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", raising=False)

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
