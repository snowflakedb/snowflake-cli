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

# ruff: noqa: SLF001
"""
Tests for configuration resolution logger module.

This tests the internal resolution logging utilities that are independent
of CLI commands and can be used in any context.
"""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from snowflake.cli.api.config_ng.resolution_logger import (
    check_value_source,
    explain_configuration,
    export_resolution_history,
    format_summary_for_display,
    get_resolution_summary,
    get_resolver,
    is_resolution_logging_available,
    show_all_resolution_chains,
    show_resolution_chain,
)
from snowflake.cli.api.config_provider import (
    ALTERNATIVE_CONFIG_ENV_VAR,
    AlternativeConfigProvider,
    reset_config_provider,
)


class TestResolutionLoggingAvailability:
    """Tests for checking if resolution logging is available."""

    def test_logging_not_available_with_legacy_provider(self):
        """Test that logging is not available with legacy provider."""
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]
            reset_config_provider()

            assert not is_resolution_logging_available()

    def test_logging_available_with_alternative_provider(self):
        """Test that logging is available with alternative provider."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "true"}):
            reset_config_provider()

            assert is_resolution_logging_available()

    def test_get_resolver_returns_none_with_legacy(self):
        """Test that get_resolver returns None with legacy provider."""
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]
            reset_config_provider()

            resolver = get_resolver()
            assert resolver is None

    def test_get_resolver_returns_instance_with_alternative(self):
        """Test that get_resolver returns resolver with alternative provider."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "true"}):
            reset_config_provider()

            resolver = get_resolver()
            assert resolver is not None


class TestShowResolutionChain:
    """Tests for showing resolution chains."""

    def test_show_chain_with_legacy_provider_shows_warning(self, capsys):
        """Test that show_resolution_chain shows warning with legacy provider."""
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]
            reset_config_provider()

            show_resolution_chain("test_key")

            captured = capsys.readouterr()
            assert "not available" in captured.out.lower()

    def test_show_all_chains_with_legacy_provider_shows_warning(self, capsys):
        """Test that show_all_resolution_chains shows warning with legacy provider."""
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]
            reset_config_provider()

            show_all_resolution_chains()

            captured = capsys.readouterr()
            assert "not available" in captured.out.lower()


class TestResolutionSummary:
    """Tests for resolution summary functionality."""

    def test_summary_returns_none_with_legacy_provider(self):
        """Test that get_resolution_summary returns None with legacy provider."""
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]
            reset_config_provider()

            summary = get_resolution_summary()
            assert summary is None

    def test_format_summary_returns_none_with_legacy_provider(self):
        """Test that format_summary_for_display returns None with legacy provider."""
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]
            reset_config_provider()

            formatted = format_summary_for_display()
            assert formatted is None

    def test_format_summary_with_alternative_provider(self):
        """Test that format_summary_for_display returns formatted string."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "true"}):
            reset_config_provider()

            # Mock the resolver to have some data
            provider = AlternativeConfigProvider()
            provider._ensure_initialized()

            with mock.patch.object(
                provider._resolver, "get_history_summary"
            ) as mock_summary:
                mock_summary.return_value = {
                    "total_keys_resolved": 5,
                    "keys_with_overrides": 2,
                    "keys_using_defaults": 1,
                    "source_usage": {
                        "cli_arguments": 2,
                        "snowflake_cli_env": 3,
                    },
                    "source_wins": {
                        "cli_arguments": 2,
                        "snowflake_cli_env": 3,
                    },
                }

                # Need to mock the provider singleton
                with mock.patch(
                    "snowflake.cli.api.config_ng.resolution_logger.get_config_provider_singleton",
                    return_value=provider,
                ):
                    formatted = format_summary_for_display()

                    assert formatted is not None
                    assert "Total keys resolved: 5" in formatted
                    assert "Keys with overrides: 2" in formatted
                    assert "Keys using defaults: 1" in formatted
                    assert "cli_arguments" in formatted
                    assert "snowflake_cli_env" in formatted


class TestCheckValueSource:
    """Tests for checking value source."""

    def test_check_value_source_returns_none_with_legacy(self):
        """Test that check_value_source returns None with legacy provider."""
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]
            reset_config_provider()

            source = check_value_source("test_key")
            assert source is None


class TestExportResolutionHistory:
    """Tests for exporting resolution history."""

    def test_export_returns_false_with_legacy_provider(self, capsys):
        """Test that export_resolution_history returns False with legacy provider."""
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]
            reset_config_provider()

            with TemporaryDirectory() as tmpdir:
                export_path = Path(tmpdir) / "test_export.json"
                success = export_resolution_history(export_path)

                assert not success
                captured = capsys.readouterr()
                assert "not available" in captured.out.lower()

    def test_export_succeeds_with_alternative_provider(self):
        """Test that export_resolution_history succeeds with alternative provider."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "true"}):
            reset_config_provider()

            with TemporaryDirectory() as tmpdir:
                export_path = Path(tmpdir) / "test_export.json"
                success = export_resolution_history(export_path)

                assert success
                assert export_path.exists()

                # Verify JSON is valid
                import json

                with open(export_path) as f:
                    data = json.load(f)

                assert "summary" in data
                assert "histories" in data


class TestExplainConfiguration:
    """Tests for explain_configuration function."""

    def test_explain_with_legacy_provider_shows_warning(self, capsys):
        """Test that explain_configuration shows warning with legacy provider."""
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]
            reset_config_provider()

            explain_configuration()

            captured = capsys.readouterr()
            assert "not available" in captured.out.lower()

    def test_explain_specific_key(self, capsys):
        """Test explaining a specific key."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "true"}):
            reset_config_provider()

            # Just test that it doesn't crash
            # Actual display testing would require more setup
            explain_configuration(key="account")

    def test_explain_all_keys_verbose(self, capsys):
        """Test explaining all keys in verbose mode."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "true"}):
            reset_config_provider()

            # Just test that it doesn't crash
            explain_configuration(verbose=True)


class TestIntegrationWithRealConfig:
    """Integration tests with actual configuration."""

    def test_resolution_with_env_vars(self):
        """Test resolution logging with actual environment variables."""
        with mock.patch.dict(
            os.environ,
            {
                ALTERNATIVE_CONFIG_ENV_VAR: "true",
                "SNOWFLAKE_ACCOUNT": "test_account",
                "SNOWFLAKE_USER": "test_user",
            },
        ):
            reset_config_provider()

            # Verify logging is available
            assert is_resolution_logging_available()

            # Get resolver and check it has data
            resolver = get_resolver()
            assert resolver is not None

            # Force resolution
            from snowflake.cli.api.config_provider import get_config_provider_singleton

            provider = get_config_provider_singleton()
            provider.read_config()

            # Check that we can get summary
            summary = get_resolution_summary()
            assert summary is not None
            assert summary["total_keys_resolved"] > 0

    def test_check_value_source_for_env_var(self):
        """Test checking the source of an environment variable."""
        with mock.patch.dict(
            os.environ,
            {
                ALTERNATIVE_CONFIG_ENV_VAR: "true",
                "SNOWFLAKE_ACCOUNT": "test_account",
            },
        ):
            reset_config_provider()

            # Force resolution
            from snowflake.cli.api.config_provider import get_config_provider_singleton

            provider = get_config_provider_singleton()
            provider.read_config()

            # Check source
            source = check_value_source("account")
            # Should be from environment (snowflake_cli_env or similar)
            assert source is not None
            assert "env" in source.lower()
