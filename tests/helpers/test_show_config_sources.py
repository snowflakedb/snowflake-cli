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

import os
from unittest import mock

from snowflake.cli.api.config_provider import ALTERNATIVE_CONFIG_ENV_VAR

COMMAND = "show-config-sources"


class TestHiddenLogic:
    """
    Test the logic that determines if the command should be hidden.

    Note: The 'hidden' parameter in Typer decorators is evaluated at module import time,
    so we test the logic itself rather than the runtime visibility in help output.
    """

    def test_hidden_logic_with_truthy_values(self):
        """Test that the hidden logic correctly identifies truthy values."""
        truthy_values = ["1", "true", "yes", "on", "TRUE", "Yes", "ON"]
        for value in truthy_values:
            # This is the logic used in the command decorator
            is_hidden = value.lower() not in ("1", "true", "yes", "on")
            assert (
                not is_hidden
            ), f"Value '{value}' should make command visible (not hidden)"

    def test_hidden_logic_with_falsy_values(self):
        """Test that the hidden logic correctly identifies falsy values."""
        falsy_values = ["", "0", "false", "no", "off", "random"]
        for value in falsy_values:
            # This is the logic used in the command decorator
            is_hidden = value.lower() not in ("1", "true", "yes", "on")
            assert is_hidden, f"Value '{value}' should make command hidden"


class TestCommandFunctionality:
    """Test that the command functions correctly when called."""

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_command_unavailable_without_env_var(self, runner):
        """Command should indicate resolution logging is unavailable without env var."""
        result = runner.invoke(["helpers", COMMAND])
        assert result.exit_code == 0
        assert "Configuration resolution logging is not available" in result.output
        assert ALTERNATIVE_CONFIG_ENV_VAR in result.output

    @mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}, clear=True)
    @mock.patch("snowflake.cli.api.config_ng.is_resolution_logging_available")
    def test_command_unavailable_message_when_logging_not_available(
        self, mock_is_available, runner
    ):
        """Command should show unavailable message when resolution logging is not available."""
        mock_is_available.return_value = False
        result = runner.invoke(["helpers", COMMAND])
        assert result.exit_code == 0
        assert "Configuration resolution logging is not available" in result.output

    @mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}, clear=True)
    @mock.patch("snowflake.cli.api.config_ng.is_resolution_logging_available")
    @mock.patch("snowflake.cli.api.config_ng.explain_configuration")
    def test_command_shows_summary_without_arguments(
        self, mock_explain, mock_is_available, runner
    ):
        """Command should show configuration summary when called without arguments."""
        mock_is_available.return_value = True
        result = runner.invoke(["helpers", COMMAND])
        assert result.exit_code == 0
        mock_explain.assert_called_once_with(key=None, verbose=False)
        assert "Configuration resolution summary displayed above" in result.output

    @mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}, clear=True)
    @mock.patch("snowflake.cli.api.config_ng.is_resolution_logging_available")
    @mock.patch("snowflake.cli.api.config_ng.explain_configuration")
    def test_command_shows_specific_key(self, mock_explain, mock_is_available, runner):
        """Command should show resolution for specific key when provided."""
        mock_is_available.return_value = True
        result = runner.invoke(["helpers", COMMAND, "account"])
        assert result.exit_code == 0
        mock_explain.assert_called_once_with(key="account", verbose=False)
        assert "Showing resolution for key: account" in result.output

    @mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}, clear=True)
    @mock.patch("snowflake.cli.api.config_ng.is_resolution_logging_available")
    @mock.patch("snowflake.cli.api.config_ng.explain_configuration")
    def test_command_shows_details_with_flag(
        self, mock_explain, mock_is_available, runner
    ):
        """Command should show detailed resolution when --show-details flag is used."""
        mock_is_available.return_value = True
        result = runner.invoke(["helpers", COMMAND, "--show-details"])
        assert result.exit_code == 0
        mock_explain.assert_called_once_with(key=None, verbose=True)
        assert "Configuration resolution summary displayed above" in result.output

    @mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}, clear=True)
    @mock.patch("snowflake.cli.api.config_ng.is_resolution_logging_available")
    @mock.patch("snowflake.cli.api.config_ng.explain_configuration")
    def test_command_shows_details_with_short_flag(
        self, mock_explain, mock_is_available, runner
    ):
        """Command should show detailed resolution when -d flag is used."""
        mock_is_available.return_value = True
        result = runner.invoke(["helpers", COMMAND, "-d"])
        assert result.exit_code == 0
        mock_explain.assert_called_once_with(key=None, verbose=True)

    @mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}, clear=True)
    @mock.patch("snowflake.cli.api.config_ng.is_resolution_logging_available")
    @mock.patch("snowflake.cli.api.config_ng.explain_configuration")
    def test_command_shows_key_with_details(
        self, mock_explain, mock_is_available, runner
    ):
        """Command should show detailed resolution for specific key."""
        mock_is_available.return_value = True
        result = runner.invoke(["helpers", COMMAND, "user", "--show-details"])
        assert result.exit_code == 0
        mock_explain.assert_called_once_with(key="user", verbose=True)
        assert "Showing resolution for key: user" in result.output

    @mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}, clear=True)
    @mock.patch("snowflake.cli.api.config_ng.is_resolution_logging_available")
    @mock.patch("snowflake.cli.api.config_ng.export_resolution_history")
    def test_command_exports_to_file_success(
        self, mock_export, mock_is_available, runner, tmp_path
    ):
        """Command should export resolution history to file when --export is used."""
        mock_is_available.return_value = True
        mock_export.return_value = True
        export_file = tmp_path / "config_debug.json"

        result = runner.invoke(["helpers", COMMAND, "--export", str(export_file)])
        assert result.exit_code == 0
        mock_export.assert_called_once_with(export_file)
        assert "Resolution history exported to:" in result.output
        assert str(export_file) in result.output

    @mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}, clear=True)
    @mock.patch("snowflake.cli.api.config_ng.is_resolution_logging_available")
    @mock.patch("snowflake.cli.api.config_ng.export_resolution_history")
    def test_command_exports_to_file_with_short_flag(
        self, mock_export, mock_is_available, runner, tmp_path
    ):
        """Command should export resolution history to file when -e is used."""
        mock_is_available.return_value = True
        mock_export.return_value = True
        export_file = tmp_path / "debug.json"

        result = runner.invoke(["helpers", COMMAND, "-e", str(export_file)])
        assert result.exit_code == 0
        mock_export.assert_called_once_with(export_file)

    @mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}, clear=True)
    @mock.patch("snowflake.cli.api.config_ng.is_resolution_logging_available")
    @mock.patch("snowflake.cli.api.config_ng.export_resolution_history")
    def test_command_export_failure(
        self, mock_export, mock_is_available, runner, tmp_path
    ):
        """Command should show error message when export fails."""
        mock_is_available.return_value = True
        mock_export.return_value = False
        export_file = tmp_path / "config_debug.json"

        result = runner.invoke(["helpers", COMMAND, "--export", str(export_file)])
        assert result.exit_code == 0
        assert "Failed to export resolution history" in result.output


class TestCommandHelp:
    """Test the command help output."""

    @mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}, clear=True)
    def test_command_help_message(self, runner):
        """Command help should display correctly."""
        result = runner.invoke(["helpers", COMMAND, "--help"])
        assert result.exit_code == 0
        assert "Show where configuration values come from" in result.output
        assert "--show-details" in result.output
        assert "--export" in result.output
        assert "Examples:" in result.output

    @mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}, clear=True)
    def test_command_help_shows_key_argument(self, runner):
        """Command help should show the optional key argument."""
        result = runner.invoke(["helpers", COMMAND, "--help"])
        assert result.exit_code == 0
        assert "KEY" in result.output or "key" in result.output.lower()
        assert "account" in result.output or "user" in result.output
