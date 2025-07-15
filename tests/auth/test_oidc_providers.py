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
    auto_detect_oidc_provider,
    get_oidc_provider,
    list_oidc_providers,
)
from snowflake.cli.api.exceptions import CliError


class TestGitHubOidcProvider:
    """Test cases for GitHubOidcProvider."""

    def test_provider_name(self):
        """Test that provider name is correct."""
        provider = GitHubOidcProvider()
        assert provider.provider_name == OidcProviderType.GITHUB.value

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "true"})
    @patch(
        "snowflake.cli._plugins.auth.workload_identity.oidc_providers.importlib.import_module"
    )
    def test_is_available_success(self, mock_import):
        """Test is_available returns True when in GitHub Actions with credentials."""
        # Mock the id package and its detect_credentials function
        mock_id_module = Mock()
        mock_credentials = Mock()
        mock_credentials.token = "mock_token"
        mock_id_module.detect_credentials.return_value = mock_credentials

        # Mock the import to return our mocked id module
        with patch("builtins.__import__", return_value=mock_id_module):
            provider = GitHubOidcProvider()
            assert provider.is_available is True

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "false"})
    def test_is_available_not_github_actions(self):
        """Test is_available returns False when not in GitHub Actions."""
        provider = GitHubOidcProvider()
        assert provider.is_available is False

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "true"})
    def test_is_available_import_error(self):
        """Test is_available returns False when id package is not available."""
        with patch(
            "builtins.__import__", side_effect=ImportError("No module named 'id'")
        ):
            provider = GitHubOidcProvider()
            assert provider.is_available is False

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "true"})
    @patch(
        "snowflake.cli._plugins.auth.workload_identity.oidc_providers.oidc_id.detect_credential"
    )
    def test_is_available_no_credentials(self, mock_detect_credential):
        """Test is_available returns False when no credentials detected."""
        mock_detect_credential.return_value = None

        provider = GitHubOidcProvider()
        assert provider.is_available is False

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "true"})
    @patch(
        "snowflake.cli._plugins.auth.workload_identity.oidc_providers.oidc_id.detect_credential"
    )
    def test_is_available_exception(self, mock_detect_credential):
        """Test is_available returns False when exception occurs."""
        mock_detect_credential.side_effect = Exception("Some error")

        provider = GitHubOidcProvider()
        assert provider.is_available is False

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.oidc_providers.oidc_id.detect_credential"
    )
    def test_get_token_import_error(self, mock_detect_credential):
        """Test get_token raises CliError when detect_credential fails."""
        mock_detect_credential.side_effect = Exception("Detection failed")

        provider = GitHubOidcProvider()
        with pytest.raises(CliError, match="Failed to detect OIDC credentials"):
            provider.get_token()

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.oidc_providers.oidc_id.detect_credential"
    )
    def test_get_token_no_credentials(self, mock_detect_credential):
        """Test get_token raises CliError when no credentials detected."""
        mock_detect_credential.return_value = None

        provider = GitHubOidcProvider()
        with pytest.raises(CliError, match="No OIDC credentials detected"):
            provider.get_token()

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.oidc_providers.oidc_id.detect_credential"
    )
    def test_get_token_success(self, mock_detect_credential):
        """Test get_token returns token when credentials are available."""
        mock_detect_credential.return_value = "mock_token_value"

        provider = GitHubOidcProvider()
        token = provider.get_token()
        assert token == "mock_token_value"
        mock_detect_credential.assert_called_once_with("https://snowflake.com")

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.oidc_providers.oidc_id.detect_credential"
    )
    def test_get_token_exception(self, mock_detect_credential):
        """Test get_token raises CliError when exception occurs."""
        mock_detect_credential.side_effect = Exception("Detection failed")

        provider = GitHubOidcProvider()
        with pytest.raises(
            CliError, match="Failed to detect OIDC credentials: Detection failed"
        ):
            provider.get_token()

    def test_get_token_info_success(self):
        """Test get_token_info returns info when credentials are available."""
        mock_id_module = Mock()
        mock_credentials = Mock()
        mock_credentials.token = "mock_token"
        mock_id_module.detect_credentials.return_value = mock_credentials

        with patch("builtins.__import__", return_value=mock_id_module):
            provider = GitHubOidcProvider()
            info = provider.get_token_info()

            expected_info = {
                "issuer": "https://token.actions.githubusercontent.com",
                "provider": OidcProviderType.GITHUB.value,
                "token_present": "true",
            }
            assert info == expected_info

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.oidc_providers.oidc_id.detect_credential"
    )
    def test_get_token_info_no_credentials(self, mock_detect_credential):
        """Test get_token_info returns empty dict when no credentials."""
        mock_detect_credential.return_value = None

        provider = GitHubOidcProvider()
        info = provider.get_token_info()
        assert info == {}

    def test_get_token_info_exception(self):
        """Test get_token_info returns empty dict when exception occurs."""
        with patch("builtins.__import__", side_effect=Exception("Import failed")):
            provider = GitHubOidcProvider()
            info = provider.get_token_info()
            assert info == {}


class TestOidcProviderRegistry:
    """Test cases for OidcProviderRegistry."""

    def test_registry_initialization(self):
        """Test that registry initializes and auto-discovers providers."""
        registry = OidcProviderRegistry()

        # Should have discovered the GitHub provider
        provider_names = registry.list_provider_names()
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

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "true"})
    def test_get_available_providers_with_github(self):
        """Test getting available providers when GitHub is available."""
        mock_id_module = Mock()
        mock_credentials = Mock()
        mock_credentials.token = "mock_token"
        mock_id_module.detect_credentials.return_value = mock_credentials

        with patch("builtins.__import__", return_value=mock_id_module):
            registry = OidcProviderRegistry()
            available_providers = registry.get_available_providers()

            assert len(available_providers) == 1
            assert available_providers[0].provider_name == OidcProviderType.GITHUB.value

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "false"})
    def test_get_available_providers_none_available(self):
        """Test getting available providers when none are available."""
        registry = OidcProviderRegistry()
        available_providers = registry.get_available_providers()

        assert len(available_providers) == 0

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "true"})
    def test_auto_detect_provider_success(self):
        """Test auto-detecting a provider when one is available."""
        mock_id_module = Mock()
        mock_credentials = Mock()
        mock_credentials.token = "mock_token"
        mock_id_module.detect_credentials.return_value = mock_credentials

        with patch("builtins.__import__", return_value=mock_id_module):
            registry = OidcProviderRegistry()
            provider = registry.auto_detect_provider()

            assert provider is not None
            assert provider.provider_name == OidcProviderType.GITHUB.value

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "false"})
    def test_auto_detect_provider_none_available(self):
        """Test auto-detecting a provider when none are available."""
        registry = OidcProviderRegistry()
        provider = registry.auto_detect_provider()

        assert provider is None

    def test_list_provider_names(self):
        """Test listing all provider names."""
        registry = OidcProviderRegistry()
        provider_names = registry.list_provider_names()

        assert OidcProviderType.GITHUB.value in provider_names


class TestModuleFunctions:
    """Test cases for module-level functions."""

    def test_get_oidc_provider(self):
        """Test get_oidc_provider function."""
        provider = get_oidc_provider(OidcProviderType.GITHUB.value)
        assert provider is not None
        assert provider.provider_name == OidcProviderType.GITHUB.value

    def test_get_oidc_provider_non_existing(self):
        """Test get_oidc_provider with non-existing provider."""
        provider = get_oidc_provider("non_existing")
        assert provider is None

    @patch.dict(os.environ, {"GITHUB_ACTIONS": "true"})
    def test_auto_detect_oidc_provider_success(self):
        """Test auto_detect_oidc_provider when provider is available."""
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
        """Test auto_detect_oidc_provider when no providers are available."""
        provider = auto_detect_oidc_provider()
        assert provider is None

    def test_list_oidc_providers(self):
        """Test list_oidc_providers function."""
        provider_names = list_oidc_providers()

        assert OidcProviderType.GITHUB.value in provider_names


class TestAbstractBaseClass:
    """Test cases for the abstract base class."""

    def test_cannot_instantiate_abstract_class(self):
        """Test that OidcTokenProvider cannot be instantiated directly."""
        with pytest.raises(TypeError):
            OidcTokenProvider()

    def test_concrete_class_must_implement_all_methods(self):
        """Test that concrete classes must implement all abstract methods."""

        class IncompleteProvider(OidcTokenProvider):
            @property
            def provider_name(self) -> str:
                return "incomplete"

            # Missing other required methods

        with pytest.raises(TypeError):
            IncompleteProvider()
