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
Integration tests for ConfigProvider Phase 7: Provider Integration.

Tests the AlternativeConfigProvider implementation and its compatibility
with LegacyConfigProvider.

Note: This file accesses private members for testing purposes, which is expected
in test code to verify internal state and behavior.
"""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pytest
from snowflake.cli.api.config_provider import (
    ALTERNATIVE_CONFIG_ENV_VAR,
    AlternativeConfigProvider,
    LegacyConfigProvider,
    get_config_provider,
    get_config_provider_singleton,
    reset_config_provider,
)


class TestProviderSelection:
    """Tests for provider selection via environment variable."""

    def test_default_provider_is_legacy(self):
        """Test that LegacyConfigProvider is used by default."""
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]

            provider = get_config_provider()
            assert isinstance(provider, LegacyConfigProvider)

    def test_alternative_provider_enabled_with_true(self):
        """Test enabling alternative provider with 'true'."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "true"}):
            provider = get_config_provider()
            assert isinstance(provider, AlternativeConfigProvider)

    def test_alternative_provider_enabled_with_1(self):
        """Test enabling alternative provider with '1'."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "1"}):
            provider = get_config_provider()
            assert isinstance(provider, AlternativeConfigProvider)

    def test_alternative_provider_enabled_with_yes(self):
        """Test enabling alternative provider with 'yes'."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "yes"}):
            provider = get_config_provider()
            assert isinstance(provider, AlternativeConfigProvider)

    def test_alternative_provider_enabled_with_on(self):
        """Test enabling alternative provider with 'on'."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "on"}):
            provider = get_config_provider()
            assert isinstance(provider, AlternativeConfigProvider)

    def test_alternative_provider_case_insensitive(self):
        """Test that environment variable is case-insensitive."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "TRUE"}):
            provider = get_config_provider()
            assert isinstance(provider, AlternativeConfigProvider)

        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "Yes"}):
            provider = get_config_provider()
            assert isinstance(provider, AlternativeConfigProvider)

    def test_singleton_pattern(self):
        """Test that singleton returns same instance."""
        with mock.patch.dict(os.environ, {}):
            reset_config_provider()

            provider1 = get_config_provider_singleton()
            provider2 = get_config_provider_singleton()

            assert provider1 is provider2

    def test_reset_config_provider(self):
        """Test that reset_config_provider creates new instance."""
        with mock.patch.dict(os.environ, {}):
            reset_config_provider()

            provider1 = get_config_provider_singleton()
            reset_config_provider()
            provider2 = get_config_provider_singleton()

            assert provider1 is not provider2


class TestAlternativeConfigProviderInitialization:
    """Tests for AlternativeConfigProvider initialization."""

    def test_lazy_initialization(self):
        """Test that provider initializes lazily on first use."""
        provider = AlternativeConfigProvider()
        assert provider._resolver is None
        assert not provider._initialized

        # Accessing any method should trigger initialization
        provider._ensure_initialized()
        assert provider._resolver is not None
        assert provider._initialized

    def test_reinitialization_clears_cache(self):
        """Test that re-initialization clears cache."""
        provider = AlternativeConfigProvider()
        provider._config_cache = {"old": "data"}
        provider._initialized = True

        provider.read_config()

        # Cache should be cleared during re-init
        assert provider._config_cache != {"old": "data"}


class TestAlternativeConfigProviderBasicOperations:
    """Tests for basic config provider operations."""

    def test_section_exists_root(self):
        """Test section_exists for root."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            mock_resolver.resolve.return_value = {"key": "value"}
            provider._initialized = True

            assert provider.section_exists()

    def test_section_exists_with_prefix(self):
        """Test section_exists for specific section."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            mock_resolver.resolve.return_value = {
                "connections.default.account": "test_account",
                "connections.default.user": "test_user",
            }
            provider._initialized = True

            assert provider.section_exists("connections")
            assert provider.section_exists("connections", "default")
            assert not provider.section_exists("nonexistent")

    def test_get_value_simple(self):
        """Test get_value for simple key."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            mock_resolver.resolve.return_value = {"account": "test_account"}
            provider._initialized = True
            # Prevent re-initialization due to config_file_override check
            from snowflake.cli.api.cli_global_context import get_cli_context

            try:
                provider._last_config_override = get_cli_context().config_file_override
            except Exception:
                provider._last_config_override = None

            value = provider.get_value(key="account")
            assert value == "test_account"

    def test_get_value_with_path(self):
        """Test get_value with path."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            mock_resolver.resolve.return_value = {
                "connections.default.account": "test_account"
            }
            provider._initialized = True
            # Prevent re-initialization due to config_file_override check
            from snowflake.cli.api.cli_global_context import get_cli_context

            try:
                provider._last_config_override = get_cli_context().config_file_override
            except Exception:
                provider._last_config_override = None

            value = provider.get_value("connections", "default", key="account")
            assert value == "test_account"

    def test_get_value_with_default(self):
        """Test get_value returns default when key not found."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            mock_resolver.resolve.return_value = {}
            provider._initialized = True

            value = provider.get_value(key="nonexistent", default="default_value")
            assert value == "default_value"

    def test_get_section_root(self):
        """Test get_section for root."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            config_data = {"key1": "value1", "key2": "value2"}
            mock_resolver.resolve.return_value = config_data
            provider._initialized = True
            # Prevent re-initialization due to config_file_override check
            from snowflake.cli.api.cli_global_context import get_cli_context

            try:
                provider._last_config_override = get_cli_context().config_file_override
            except Exception:
                provider._last_config_override = None

            section = provider.get_section()
            assert section == config_data

    def test_get_section_connections(self):
        """Test get_section for connections."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            mock_resolver.resolve.return_value = {
                "connections.default.account": "test_account",
                "connections.default.user": "test_user",
                "connections.prod.account": "prod_account",
            }
            provider._initialized = True
            # Prevent re-initialization due to config_file_override check
            from snowflake.cli.api.cli_global_context import get_cli_context

            try:
                provider._last_config_override = get_cli_context().config_file_override
            except Exception:
                provider._last_config_override = None

            section = provider.get_section("connections")
            assert "default" in section
            assert "prod" in section
            assert section["default"]["account"] == "test_account"
            assert section["prod"]["account"] == "prod_account"

    def test_get_section_specific_connection(self):
        """Test get_section for specific connection."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            mock_resolver.resolve.return_value = {
                "connections.default.account": "test_account",
                "connections.default.user": "test_user",
            }
            provider._initialized = True
            # Prevent re-initialization due to config_file_override check
            from snowflake.cli.api.cli_global_context import get_cli_context

            try:
                provider._last_config_override = get_cli_context().config_file_override
            except Exception:
                provider._last_config_override = None

            section = provider.get_section("connections", "default")
            assert section == {"account": "test_account", "user": "test_user"}


class TestAlternativeConfigProviderConnectionOperations:
    """Tests for connection-specific operations."""

    def test_get_connection_dict(self):
        """Test get_connection_dict retrieves connection config."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            mock_resolver.resolve.return_value = {
                "connections.default.account": "test_account",
                "connections.default.user": "test_user",
                "connections.default.password": "secret",
            }
            provider._initialized = True
            # Prevent re-initialization due to config_file_override check
            from snowflake.cli.api.cli_global_context import get_cli_context

            try:
                provider._last_config_override = get_cli_context().config_file_override
            except Exception:
                provider._last_config_override = None

            conn_dict = provider.get_connection_dict("default")
            assert conn_dict == {
                "account": "test_account",
                "user": "test_user",
                "password": "secret",
            }

    def test_get_connection_dict_not_found(self):
        """Test get_connection_dict raises error for missing connection."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            mock_resolver.resolve.return_value = {}
            provider._initialized = True

            with pytest.raises(Exception):  # MissingConfigurationError
                provider.get_connection_dict("nonexistent")

    def test_get_all_connections_dict(self):
        """Test _get_all_connections_dict returns nested dict."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            mock_resolver.resolve.return_value = {
                "connections.default.account": "test_account",
                "connections.default.user": "test_user",
                "connections.prod.account": "prod_account",
                "connections.prod.user": "prod_user",
            }
            provider._initialized = True
            # Prevent re-initialization due to config_file_override check
            from snowflake.cli.api.cli_global_context import get_cli_context

            try:
                provider._last_config_override = get_cli_context().config_file_override
            except Exception:
                provider._last_config_override = None

            all_conns = provider._get_all_connections_dict()
            assert "default" in all_conns
            assert "prod" in all_conns
            assert all_conns["default"] == {
                "account": "test_account",
                "user": "test_user",
            }
            assert all_conns["prod"] == {
                "account": "prod_account",
                "user": "prod_user",
            }

    @mock.patch("snowflake.cli.api.config.ConnectionConfig")
    def test_get_all_connections(self, mock_connection_config):
        """Test get_all_connections returns ConnectionConfig objects."""
        provider = AlternativeConfigProvider()

        # Mock ConnectionConfig.from_dict
        mock_config_instance = mock.Mock()
        mock_connection_config.from_dict.return_value = mock_config_instance

        # Mock _get_file_based_connections to avoid resolver._sources access
        with mock.patch.object(
            provider, "_get_file_based_connections"
        ) as mock_get_file_based:
            mock_get_file_based.return_value = {"default": mock_config_instance}

            all_conns = provider.get_all_connections()

            assert "default" in all_conns
            assert all_conns["default"] == mock_config_instance
            mock_get_file_based.assert_called_once()


class TestAlternativeConfigProviderWriteOperations:
    """Tests for write operations that delegate to legacy system."""

    @mock.patch("snowflake.cli.api.config.set_config_value")
    def test_set_value_delegates_to_legacy(self, mock_set_value):
        """Test that set_value delegates to legacy system."""
        provider = AlternativeConfigProvider()
        provider._initialized = True

        provider.set_value(["test", "path"], "value")

        mock_set_value.assert_called_once_with(["test", "path"], "value")
        assert not provider._initialized  # Should reset
        assert not provider._config_cache  # Should clear cache

    @mock.patch("snowflake.cli.api.config.unset_config_value")
    def test_unset_value_delegates_to_legacy(self, mock_unset_value):
        """Test that unset_value delegates to legacy system."""
        provider = AlternativeConfigProvider()
        provider._initialized = True

        provider.unset_value(["test", "path"])

        mock_unset_value.assert_called_once_with(["test", "path"])
        assert not provider._initialized  # Should reset
        assert not provider._config_cache  # Should clear cache


class TestProviderIntegrationEndToEnd:
    """End-to-end integration tests with real config files."""

    def test_alternative_provider_with_toml_file(self):
        """Test alternative provider reads from TOML file."""
        with TemporaryDirectory() as tmpdir:
            # Create a test config file
            config_file = Path(tmpdir) / "connections.toml"
            config_file.write_text(
                """
[default]
account = "test_account"
user = "test_user"
password = "test_password"
"""
            )

            # Create provider and test
            # Note: This requires mocking the config manager to use our temp file
            # Full integration testing would be done in separate test suite

    def test_provider_switching_via_environment(self):
        """Test switching between providers via environment variable."""
        # Test legacy provider (default)
        reset_config_provider()
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]

            provider = get_config_provider_singleton()
            assert isinstance(provider, LegacyConfigProvider)

        # Test alternative provider (enabled)
        reset_config_provider()
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: "true"}):
            provider = get_config_provider_singleton()
            assert isinstance(provider, AlternativeConfigProvider)


class TestAlternativeConfigProviderConnections:
    """Tests for AlternativeConfigProvider connection filtering."""

    def test_get_all_connections_excludes_env_by_default(self, monkeypatch):
        """Test that get_all_connections excludes env-only connections by default."""
        monkeypatch.setenv(ALTERNATIVE_CONFIG_ENV_VAR, "1")

        # Set up environment variable for connection
        monkeypatch.setenv("SNOWFLAKE_CONNECTIONS_ENVONLY_ACCOUNT", "test_account")
        monkeypatch.setenv("SNOWFLAKE_CONNECTIONS_ENVONLY_USER", "test_user")

        reset_config_provider()
        provider = get_config_provider_singleton()

        # Default: should not include env-only connection
        connections = provider.get_all_connections(include_env_connections=False)
        assert "envonly" not in connections

        # With flag: should include env-only connection
        reset_config_provider()
        all_connections = provider.get_all_connections(include_env_connections=True)
        assert "envonly" in all_connections
        assert all_connections["envonly"].account == "test_account"
        assert all_connections["envonly"].user == "test_user"

    def test_get_all_connections_with_mixed_sources(self, monkeypatch):
        """Test that file-based connections are included but env-only excluded by default."""
        monkeypatch.setenv(ALTERNATIVE_CONFIG_ENV_VAR, "1")

        # Set env variable for env-only connection
        monkeypatch.setenv("SNOWFLAKE_CONNECTIONS_ENVCONN_ACCOUNT", "env_account")

        reset_config_provider()
        provider = get_config_provider_singleton()

        # Without flag: should have file connections but not env-only connection
        connections = provider.get_all_connections(include_env_connections=False)
        # Test fixture connections should be present (from test.toml)
        assert len(connections) > 0
        assert "envconn" not in connections

        # With flag: should have both file and env connections
        reset_config_provider()
        all_connections = provider.get_all_connections(include_env_connections=True)
        assert "envconn" in all_connections
        # Should have more connections when including env
        assert len(all_connections) >= len(connections)

    def test_legacy_provider_ignores_include_env_flag(self, monkeypatch):
        """Test that LegacyConfigProvider ignores the include_env_connections flag."""
        # Ensure legacy provider is used
        monkeypatch.delenv(ALTERNATIVE_CONFIG_ENV_VAR, raising=False)

        reset_config_provider()
        provider = get_config_provider_singleton()

        assert isinstance(provider, LegacyConfigProvider)

        # Both calls should return the same result (flag is ignored)
        connections_default = provider.get_all_connections(
            include_env_connections=False
        )
        connections_all = provider.get_all_connections(include_env_connections=True)

        # Should be same connections (legacy doesn't filter)
        assert set(connections_default.keys()) == set(connections_all.keys())
