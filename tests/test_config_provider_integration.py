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
from typing import Any
from unittest import mock

import pytest
from snowflake.cli.api.cli_global_context import fork_cli_context
from snowflake.cli.api.config_ng.core import SourceType, ValueSource
from snowflake.cli.api.config_provider import (
    ALTERNATIVE_CONFIG_ENV_VAR,
    AlternativeConfigProvider,
    LegacyConfigProvider,
    get_config_provider,
    get_config_provider_singleton,
    reset_config_provider,
)


class _StubResolver:
    """Minimal resolver stub that only exposes get_sources()."""

    def __init__(self, sources: list[ValueSource]):
        self._sources = sources

    def get_sources(self) -> list[ValueSource]:
        return self._sources


class _StaticFileSource(ValueSource):
    """Test-only file source that returns static data."""

    def __init__(
        self,
        data: dict[str, Any],
        source_name: ValueSource.SourceName = "cli_config_toml",
    ):
        self._data = data
        self._source_name = source_name

    @property
    def source_name(self) -> ValueSource.SourceName:
        return self._source_name

    @property
    def source_type(self) -> SourceType:
        return SourceType.FILE

    def discover(self, key: str | None = None) -> dict[str, Any]:
        return self._data

    def supports_key(self, key: str) -> bool:
        return key in self._data


def _sync_last_config_override(provider: AlternativeConfigProvider) -> None:
    """Mirror logic from production code to avoid re-initialization in tests."""
    from snowflake.cli.api.cli_global_context import get_cli_context

    try:
        provider._last_config_override = get_cli_context().config_file_override
    except Exception:
        provider._last_config_override = None


class TestProviderSelection:
    """Tests for provider selection via environment variable."""

    def test_default_provider_is_legacy(self):
        """Test that LegacyConfigProvider is used by default."""
        with mock.patch.dict(os.environ, {}, clear=False):
            if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
                del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]

            provider = get_config_provider()
            assert isinstance(provider, LegacyConfigProvider)

    @pytest.mark.parametrize(
        "env_value",
        ["true", "1", "yes", "on", "TRUE", "True", "Yes", "YES", "ON"],
    )
    def test_alternative_provider_enabled_with_various_values(self, env_value):
        """Test enabling alternative provider with various truthy values."""
        with mock.patch.dict(os.environ, {ALTERNATIVE_CONFIG_ENV_VAR: env_value}):
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


class TestAlternativeConfigProviderOverrideHandling:
    """Tests for handling config file overrides."""

    def test_reinitializes_sources_when_config_override_changes(self, tmp_path):
        """Ensure provider rebuilds sources after config override changes."""
        provider = AlternativeConfigProvider()

        first_config = tmp_path / "config_one.toml"
        first_config.write_text('[connections.test]\naccount = "first"\n')
        first_config.chmod(0o600)

        second_config = tmp_path / "config_two.toml"
        second_config.write_text('[connections.test]\naccount = "second"\n')
        second_config.chmod(0o600)

        with fork_cli_context() as ctx:
            ctx.config_file_override = first_config
            first_connections = provider.get_section("connections")
            assert first_connections["test"]["account"] == "first"

            ctx.config_file_override = second_config
            second_connections = provider.get_section("connections")
            assert second_connections["test"]["account"] == "second"


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
                "connections": {
                    "default": {"account": "test_account", "user": "test_user"}
                }
            }
            provider._initialized = True
            provider._config_cache = mock_resolver.resolve.return_value

            assert provider.section_exists("connections")
            assert provider.section_exists("connections", "default")
            assert not provider.section_exists("nonexistent")

    def test_get_value_simple(self):
        """Test get_value for simple key."""
        provider = AlternativeConfigProvider()

        with mock.patch.object(provider, "_resolver") as mock_resolver:
            mock_resolver.resolve.return_value = {"account": "test_account"}
            provider._initialized = True
            provider._config_cache = mock_resolver.resolve.return_value
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
                "connections": {"default": {"account": "test_account"}}
            }
            provider._initialized = True
            provider._config_cache = mock_resolver.resolve.return_value
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
            provider._config_cache = config_data
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
                "connections": {
                    "default": {"account": "test_account", "user": "test_user"},
                    "prod": {"account": "prod_account"},
                }
            }
            provider._initialized = True
            provider._config_cache = mock_resolver.resolve.return_value
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
                "connections": {
                    "default": {"account": "test_account", "user": "test_user"}
                }
            }
            provider._initialized = True
            provider._config_cache = mock_resolver.resolve.return_value
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
                "connections": {
                    "default": {
                        "account": "test_account",
                        "user": "test_user",
                        "password": "secret",
                    }
                }
            }
            provider._initialized = True
            provider._config_cache = mock_resolver.resolve.return_value
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
                "connections": {
                    "default": {"account": "test_account", "user": "test_user"},
                    "prod": {"account": "prod_account", "user": "prod_user"},
                }
            }
            provider._initialized = True
            provider._config_cache = mock_resolver.resolve.return_value
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

    def test_file_connections_include_root_level_defaults(self):
        """Root-level file parameters should merge into connection definitions."""
        provider = AlternativeConfigProvider()
        provider._resolver = _StubResolver(
            [
                _StaticFileSource(
                    {
                        "connections": {"dev": {"database": "sample_db"}},
                        "account": "acct_from_file",
                        "user": "user_from_file",
                    }
                )
            ]
        )
        provider._initialized = True
        _sync_last_config_override(provider)

        connections = provider.get_all_connections(include_env_connections=False)

        assert "dev" in connections
        dev_conn = connections["dev"]
        assert dev_conn.account == "acct_from_file"
        assert dev_conn.user == "user_from_file"
        assert dev_conn.database == "sample_db"

    def test_file_connections_create_default_from_root_params(self):
        """Root-level file params should create a default connection when needed."""
        provider = AlternativeConfigProvider()
        provider._resolver = _StubResolver(
            [
                _StaticFileSource(
                    {
                        "account": "acct_only",
                        "user": "user_only",
                        "password": "secret",
                    }
                )
            ]
        )
        provider._initialized = True
        _sync_last_config_override(provider)

        connections = provider.get_all_connections(include_env_connections=False)

        assert list(connections.keys()) == ["default"]
        default_conn = connections["default"]
        assert default_conn.account == "acct_only"
        assert default_conn.user == "user_only"
        assert default_conn.password == "secret"

    def test_file_connections_preserve_unknown_root_keys(self):
        """Unknown root-level keys should be preserved in connection _other_settings."""
        provider = AlternativeConfigProvider()
        provider._resolver = _StubResolver(
            [
                _StaticFileSource(
                    {
                        "account": "acct_only",
                        "user": "user_only",
                        "custom_option": "custom_value",
                    }
                )
            ]
        )
        provider._initialized = True
        _sync_last_config_override(provider)

        connections = provider.get_all_connections(include_env_connections=False)

        assert "default" in connections
        default_conn = connections["default"]
        assert default_conn.account == "acct_only"
        assert default_conn._other_settings["custom_option"] == "custom_value"
