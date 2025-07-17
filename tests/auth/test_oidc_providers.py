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
from unittest.mock import Mock, patch

import pytest
from snowflake.cli._plugins.auth.workload_identity.oidc_providers import (
    GitHubOidcProvider,
    OidcProviderRegistry,
    OidcProviderType,
    OidcTokenProvider,
    _registry,
    auto_detect_oidc_provider,
    get_oidc_provider,
)
from snowflake.cli.api.exceptions import CliError


class TestGitHubOidcProvider:
    """Test cases for GitHubOidcProvider."""

    def test_provider_name(self):
        """Test that provider returns correct name."""
        provider = GitHubOidcProvider()
        assert provider.provider_name == OidcProviderType.GITHUB.value

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "true"})
    def test_is_available_success(self):
        """Test is_available when GITHUB_ACTIONS is true."""
        provider = GitHubOidcProvider()
        assert provider.is_available is True

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}, clear=True)
    def test_is_available_github_actions_true(self):
        """Test is_available when only GITHUB_ACTIONS is set to true."""
        provider = GitHubOidcProvider()
        assert provider.is_available is True

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "false"})
    def test_is_available_not_github_actions(self):
        """Test is_available when GITHUB_ACTIONS is false."""
        provider = GitHubOidcProvider()
        assert provider.is_available is False

    @patch("snowflake.cli._plugins.auth.workload_identity.oidc_providers.oidc_id")
    def test_get_token_success(self, mock_oidc_id):
        """Test get_token when credentials are available."""
        mock_oidc_id.detect_credential.return_value = "mock_token"

        provider = GitHubOidcProvider()
        token = provider.get_token()

        assert token == "mock_token"
        mock_oidc_id.detect_credential.assert_called_once_with("snowflakecomputing.com")

    @patch("snowflake.cli._plugins.auth.workload_identity.oidc_providers.oidc_id")
    def test_get_token_no_credentials(self, mock_oidc_id):
        """Test get_token when no credentials are detected."""
        mock_oidc_id.detect_credential.return_value = None

        provider = GitHubOidcProvider()

        with pytest.raises(CliError, match="No OIDC credentials detected"):
            provider.get_token()

    @patch("snowflake.cli._plugins.auth.workload_identity.oidc_providers.oidc_id")
    def test_get_token_exception(self, mock_oidc_id):
        """Test get_token when an exception occurs."""
        mock_oidc_id.detect_credential.side_effect = Exception("Detection failed")

        provider = GitHubOidcProvider()

        with pytest.raises(CliError, match="Failed to detect OIDC credentials"):
            provider.get_token()

    @patch("snowflake.cli._plugins.auth.workload_identity.oidc_providers.oidc_id")
    def test_get_token_import_error(self, mock_oidc_id):
        """Test get_token when import fails."""
        mock_oidc_id.detect_credential.side_effect = ImportError("Module not found")

        provider = GitHubOidcProvider()

        with pytest.raises(CliError, match="Failed to detect OIDC credentials"):
            provider.get_token()


class TestOidcProviderRegistry:
    """Test cases for OidcProviderRegistry."""

    def test_registry_initialization(self):
        """Test that registry initializes and auto-discovers providers."""
        registry = OidcProviderRegistry()

        # Should have discovered the GitHub provider
        provider_names = registry.provider_names
        assert OidcProviderType.GITHUB.value in provider_names

    def test_get_provider_existing(self):
        """Test getting an existing provider."""
        registry = OidcProviderRegistry()
        provider = registry.get_provider(OidcProviderType.GITHUB.value)

        assert provider is not None
        assert provider.provider_name == OidcProviderType.GITHUB.value

    def test_get_provider_non_existing(self):
        """Test getting a non-existing provider."""
        registry = OidcProviderRegistry()
        provider = registry.get_provider("non_existing")

        assert provider is None

    def test_register_provider(self):
        """Test manually registering a provider."""

        class MockProvider(OidcTokenProvider):
            @property
            def provider_name(self) -> str:
                return "mock"

            @property
            def is_available(self) -> bool:
                return True

            def get_token(self) -> str:
                return "mock_token"

            def get_token_info(self) -> dict:
                return {"provider": "mock"}

        registry = OidcProviderRegistry()
        registry.register_provider(MockProvider)

        # Should be able to get the registered provider
        provider = registry.get_provider("mock")
        assert provider is not None
        assert provider.provider_name == "mock"

    def test_provider_names_property(self):
        """Test provider_names property returns all registered provider names."""
        registry = OidcProviderRegistry()
        provider_names = registry.provider_names

        assert OidcProviderType.GITHUB.value in provider_names

    def test_all_providers_property(self):
        """Test all_providers property returns all registered provider instances."""
        registry = OidcProviderRegistry()
        all_providers = registry.all_providers

        # Should have at least the GitHub provider
        assert len(all_providers) >= 1
        provider_names = [p.provider_name for p in all_providers]
        assert OidcProviderType.GITHUB.value in provider_names


class TestModuleFunctions:
    """Test cases for module-level functions."""

    def test_get_oidc_provider(self):
        """Test get_oidc_provider function when provider is not available."""
        with pytest.raises(
            CliError,
            match="Provider 'github' is not available in the current environment",
        ):
            get_oidc_provider(OidcProviderType.GITHUB.value)

    def test_get_oidc_provider_non_existing(self):
        """Test get_oidc_provider with non-existing provider."""
        with pytest.raises(
            CliError,
            match="Unknown provider 'non_existing'. Available providers: github",
        ):
            get_oidc_provider("non_existing")

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "true"})
    def test_get_oidc_provider_available(self):
        """Test get_oidc_provider function when provider is available."""
        provider = get_oidc_provider(OidcProviderType.GITHUB.value)
        assert provider is not None
        assert provider.provider_name == OidcProviderType.GITHUB.value

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "true"})
    def test_auto_detect_oidc_provider_success(self):
        """Test auto_detect_oidc_provider when single provider is available."""
        mock_id_module = Mock()
        mock_credentials = Mock()
        mock_credentials.token = "mock_token"
        mock_id_module.detect_credentials.return_value = mock_credentials

        with patch("builtins.__import__", return_value=mock_id_module):
            provider = auto_detect_oidc_provider()

            assert provider is not None
            assert provider.provider_name == OidcProviderType.GITHUB.value

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "false"})
    def test_auto_detect_oidc_provider_none_available(self):
        """Test auto_detect_oidc_provider raises error when no providers are available."""
        with pytest.raises(
            CliError, match="No OIDC provider detected in current environment"
        ):
            auto_detect_oidc_provider()

    def test_auto_detect_oidc_provider_multiple_providers_error(self):
        """Test auto_detect_oidc_provider raises error when multiple providers are available."""
        # Create a mock provider that's always available
        class AlwaysAvailableProvider(OidcTokenProvider):
            @property
            def provider_name(self) -> str:
                return "always_available"

            @property
            def is_available(self) -> bool:
                return True

            def get_token(self) -> str:
                return "mock_token"

        # Mock the registry to return multiple available providers
        with patch(
            "snowflake.cli._plugins.auth.workload_identity.oidc_providers._registry"
        ) as mock_registry:
            # Create mock providers
            github_provider = Mock()
            github_provider.provider_name = "github"
            github_provider.is_available = True

            other_provider = Mock()
            other_provider.provider_name = "always_available"
            other_provider.is_available = True

            mock_registry.all_providers = [github_provider, other_provider]

            with pytest.raises(
                CliError,
                match="Multiple OIDC providers detected: github, always_available",
            ):
                auto_detect_oidc_provider()

    def test_registry_provider_names(self):
        """Test registry provider_names property."""
        provider_names = _registry.provider_names

        assert OidcProviderType.GITHUB.value in provider_names


class TestAbstractBaseClass:
    """Test cases for the abstract base class OidcTokenProvider."""

    def test_cannot_instantiate_abstract_class(self):
        """Test that we cannot instantiate the abstract OidcTokenProvider."""
        with pytest.raises(TypeError):
            OidcTokenProvider()  # type: ignore

    def test_concrete_class_must_implement_all_methods(self):
        """Test that concrete classes must implement all abstract methods."""

        class IncompleteProvider(OidcTokenProvider):
            @property
            def provider_name(self) -> str:
                return "incomplete"

            # Missing is_available and get_token

        with pytest.raises(TypeError):
            IncompleteProvider()  # type: ignore
