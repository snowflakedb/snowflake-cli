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

"""Tests for config_ng telemetry integration."""

from unittest.mock import MagicMock, patch

from snowflake.cli.api.config_ng import (
    CliParameters,
    ConfigurationResolver,
    get_config_telemetry_payload,
    record_config_source_usage,
)
from snowflake.cli.api.config_ng.sources import CliConfigFile


class TestRecordConfigSourceUsage:
    """Tests for record_config_source_usage function."""

    def test_records_winning_sources(self):
        """Test that winning sources are recorded as counters."""
        # Create resolver with some sources
        cli_config = CliConfigFile.from_string(
            """
            [connections.test]
            account = "test_account"
            user = "test_user"
            """
        )
        cli_params = CliParameters(cli_context={"password": "secret"})
        resolver = ConfigurationResolver(sources=[cli_config, cli_params])

        # Resolve to populate history
        resolver.resolve()

        # Mock CLI context
        mock_context = MagicMock()
        mock_metrics = MagicMock()
        mock_context.metrics = mock_metrics

        with patch(
            "snowflake.cli.api.cli_global_context.get_cli_context",
            return_value=mock_context,
        ):
            record_config_source_usage(resolver)

        # Verify counters were set
        assert mock_metrics.set_counter.called
        # Should have calls for all sources
        call_args = [call[0] for call in mock_metrics.set_counter.call_args_list]
        counter_fields = [arg[0] for arg in call_args]

        # Verify some expected counter fields are present
        assert any("config_source" in str(field) for field in counter_fields)

    def test_handles_no_cli_context_gracefully(self):
        """Test that function doesn't fail if CLI context unavailable."""
        cli_config = CliConfigFile.from_string(
            """
            [connections.test]
            account = "test_account"
            """
        )
        resolver = ConfigurationResolver(sources=[cli_config])
        resolver.resolve()

        with patch(
            "snowflake.cli.api.cli_global_context.get_cli_context",
            side_effect=Exception("No context"),
        ):
            # Should not raise
            record_config_source_usage(resolver)

    def test_sets_counter_to_zero_for_unused_sources(self):
        """Test that unused sources get counter value 0."""
        # Only use CLI config, not CLI params
        cli_config = CliConfigFile.from_string(
            """
            [connections.test]
            account = "test_account"
            """
        )
        resolver = ConfigurationResolver(sources=[cli_config])
        resolver.resolve()

        mock_context = MagicMock()
        mock_metrics = MagicMock()
        mock_context.metrics = mock_metrics

        with patch(
            "snowflake.cli.api.cli_global_context.get_cli_context",
            return_value=mock_context,
        ):
            record_config_source_usage(resolver)

        # Check that at least one source was set to 0
        call_args = mock_metrics.set_counter.call_args_list
        values = [call[0][1] for call in call_args]
        assert 0 in values


class TestGetConfigTelemetryPayload:
    """Tests for get_config_telemetry_payload function."""

    def test_returns_empty_dict_for_none_resolver(self):
        """Test that None resolver returns empty dict."""
        result = get_config_telemetry_payload(None)
        assert result == {}

    def test_returns_summary_data(self):
        """Test that function returns summary data from resolver."""
        cli_config = CliConfigFile.from_string(
            """
            [connections.test]
            account = "test_account"
            user = "test_user"
            """
        )
        cli_params = CliParameters(cli_context={"password": "secret"})
        resolver = ConfigurationResolver(sources=[cli_config, cli_params])

        # Resolve to populate history
        resolver.resolve()

        result = get_config_telemetry_payload(resolver)

        # Verify expected keys are present
        assert "config_sources_used" in result
        assert "config_source_wins" in result
        assert "config_total_keys_resolved" in result
        assert "config_keys_with_overrides" in result

        # Verify data types
        assert isinstance(result["config_sources_used"], list)
        assert isinstance(result["config_source_wins"], dict)
        assert isinstance(result["config_total_keys_resolved"], int)
        assert isinstance(result["config_keys_with_overrides"], int)

    def test_handles_resolver_errors_gracefully(self):
        """Test that function handles resolver errors gracefully."""
        mock_resolver = MagicMock()
        mock_resolver.get_tracker.side_effect = Exception("Tracker error")

        result = get_config_telemetry_payload(mock_resolver)
        assert result == {}

    def test_tracks_source_wins_correctly(self):
        """Test that source wins are tracked correctly."""
        cli_config = CliConfigFile.from_string(
            """
            [connections.test]
            account = "test_account"
            """
        )
        # CLI params should win for password
        cli_params = CliParameters(cli_context={"password": "override"})
        resolver = ConfigurationResolver(sources=[cli_config, cli_params])

        resolver.resolve()

        result = get_config_telemetry_payload(resolver)

        # Verify that cli_arguments won for password
        source_wins = result["config_source_wins"]
        assert "cli_arguments" in source_wins
        assert source_wins["cli_arguments"] > 0


class TestTelemetryIntegration:
    """Integration tests for telemetry system."""

    def test_telemetry_records_from_config_provider(self):
        """Test that config provider records telemetry on initialization."""
        from snowflake.cli.api.config_provider import AlternativeConfigProvider

        mock_context = MagicMock()
        mock_metrics = MagicMock()
        mock_context.metrics = mock_metrics
        mock_context.connection_context.present_values_as_dict.return_value = {}
        mock_context.config_file_override = None

        def mock_getter():
            return mock_context

        with patch(
            "snowflake.cli.api.cli_global_context.get_cli_context",
            return_value=mock_context,
        ):
            provider = AlternativeConfigProvider(cli_context_getter=mock_getter)
            provider._ensure_initialized()  # noqa: SLF001

        # Verify that telemetry was recorded
        assert mock_metrics.set_counter.called
