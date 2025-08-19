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

from enum import Enum
from unittest.mock import Mock, patch

import pytest
from snowflake.cli._app.auth.errors import (
    OidcProviderAutoDetectionError,
    OidcProviderNotFoundError,
    OidcProviderUnavailableError,
    OidcTokenRetrievalError,
)
from snowflake.cli._app.auth.oidc_providers import (
    OidcProviderType,
    OidcProviderTypeWithAuto,
)
from snowflake.cli._plugins.auth.oidc.manager import OidcManager
from snowflake.cli.api.exceptions import CliError


class TestOidcManager:
    """Test cases for OidcManager."""

    def test_read_token_with_auto_type(self):
        """Test read_token method with auto type delegates to auto_detect_oidc_provider."""
        manager = OidcManager()

        with patch(
            "snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider"
        ) as mock_auto_detect:
            mock_provider = Mock()
            mock_provider.provider_name = "github"
            mock_provider.get_token.return_value = "auto detect result"
            mock_auto_detect.return_value = mock_provider

            result = manager.read_token(OidcProviderTypeWithAuto.AUTO)

            mock_auto_detect.assert_called_once()
            mock_provider.get_token.assert_called_once()
            assert result == "auto detect result"

    def test_read_token_with_specific_type(self):
        """Test read_token method with specific provider type delegates to get_active_oidc_provider."""
        manager = OidcManager()

        with patch(
            "snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider"
        ) as mock_get_provider:
            mock_provider = Mock()
            mock_provider.provider_name = "github"
            mock_provider.get_token.return_value = "specific result"
            mock_get_provider.return_value = mock_provider

            result = manager.read_token(OidcProviderType.GITHUB)

            mock_get_provider.assert_called_once_with(OidcProviderType.GITHUB.value)
            mock_provider.get_token.assert_called_once()
            assert result == "specific result"

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_token_auto_detect_token_success(self, mock_auto_detect):
        """Test read_token with auto when provider is available and working."""
        # Create mock provider
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB
        mock_provider.get_token.return_value = "mock_token"

        mock_auto_detect.return_value = mock_provider

        manager = OidcManager()
        result = manager.read_token(OidcProviderTypeWithAuto.AUTO)

        mock_auto_detect.assert_called_once()
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_token_auto_detect_token_success_no_info(self, mock_auto_detect):
        """Test read_token with auto when provider works but has no token info."""
        # Create mock provider with no token info
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.return_value = "mock_token"
        mock_provider.get_token_info.return_value = {}

        mock_auto_detect.return_value = mock_provider

        manager = OidcManager()
        result = manager.read_token(OidcProviderTypeWithAuto.AUTO)

        mock_auto_detect.assert_called_once()
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_token_auto_detect_token_no_provider_with_available_providers(
        self, mock_auto_detect
    ):
        """Test read_token with auto when no provider detected but providers are registered."""

        error_message = "No OIDC provider detected in current environment. Available providers: github, other_provider. Use --type <provider> to specify a provider explicitly."
        mock_auto_detect.side_effect = OidcProviderAutoDetectionError(error_message)

        manager = OidcManager()

        with pytest.raises(
            CliError, match="No OIDC provider detected in current environment"
        ):
            manager.read_token(OidcProviderTypeWithAuto.AUTO)

    @patch("snowflake.cli._plugins.auth.oidc.manager.auto_detect_oidc_provider")
    def test_read_token_auto_detect_token_provider_fails(self, mock_auto_detect):
        """Test read_token with auto when provider fails to get token."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        error_message = "Token retrieval failed"
        mock_provider.get_token.side_effect = OidcTokenRetrievalError(error_message)

        mock_auto_detect.return_value = mock_provider

        manager = OidcManager()

        with pytest.raises(CliError, match=error_message):
            manager.read_token(OidcProviderTypeWithAuto.AUTO)

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_token_specific_token_success(self, mock_get_provider):
        """Test read_token with specific provider when provider exists and works."""
        # Create mock provider
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = True
        mock_provider.get_token.return_value = "mock_token"

        mock_get_provider.return_value = mock_provider

        manager = OidcManager()
        result = manager.read_token(OidcProviderType.GITHUB)

        mock_get_provider.assert_called_once_with(OidcProviderType.GITHUB.value)
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_token_specific_token_provider_not_found(self, mock_get_provider):
        """Test read_token with specific provider when provider doesn't exist."""

        class InvalidProvider(Enum):
            UNKNOWN_PROVIDER = "unknown_provider"

        error_message = f"Unknown provider {InvalidProvider.UNKNOWN_PROVIDER}. Available providers: github, other_provider"
        mock_get_provider.side_effect = OidcProviderNotFoundError(error_message)

        manager = OidcManager()

        with pytest.raises(
            CliError, match=f"Unknown provider {InvalidProvider.UNKNOWN_PROVIDER}"
        ):
            manager.read_token(InvalidProvider.UNKNOWN_PROVIDER)  # type: ignore

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_token_specific_token_provider_not_available(self, mock_get_provider):
        """Test read_token with specific provider when provider exists but is not available."""
        error_message = f"Provider '{OidcProviderType.GITHUB.value}' is not available in the current environment."
        mock_get_provider.side_effect = OidcProviderUnavailableError(error_message)

        manager = OidcManager()

        with pytest.raises(
            CliError,
            match=f"Provider '{OidcProviderType.GITHUB.value}' is not available in the current environment",
        ):
            manager.read_token(OidcProviderType.GITHUB)

    @patch("snowflake.cli._plugins.auth.oidc.manager.get_active_oidc_provider")
    def test_read_token_specific_token_provider_fails(self, mock_get_provider):
        """Test read_token with specific provider when provider exists but fails to get token."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = True
        error_message = "Token retrieval failed"
        mock_provider.get_token.side_effect = OidcTokenRetrievalError(error_message)

        mock_get_provider.return_value = mock_provider

        manager = OidcManager()

        with pytest.raises(CliError, match=error_message):
            manager.read_token(OidcProviderType.GITHUB)
