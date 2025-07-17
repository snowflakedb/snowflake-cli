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

from unittest.mock import Mock, patch

import pytest
from snowflake.cli._plugins.auth.workload_identity.manager import (
    WorkloadIdentityManager,
)
from snowflake.cli._plugins.auth.workload_identity.oidc_providers import (
    OidcProviderType,
)
from snowflake.cli.api.exceptions import CliError


class TestWorkloadIdentityManager:
    """Test cases for WorkloadIdentityManager."""

    def test_setup_not_implemented(self):
        """Test that setup method raises NotImplementedError."""
        manager = WorkloadIdentityManager()

        with pytest.raises(
            NotImplementedError,
            match="GitHub workload identity federation setup is not yet implemented",
        ):
            manager.setup("owner/repo")

    def test_read_with_auto_type(self):
        """Test read method with auto type delegates to auto_detect_oidc_provider."""
        manager = WorkloadIdentityManager()

        with patch(
            "snowflake.cli._plugins.auth.workload_identity.manager.auto_detect_oidc_provider"
        ) as mock_auto_detect:
            mock_provider = Mock()
            mock_provider.provider_name = "github"
            mock_provider.get_token.return_value = "auto detect result"
            mock_auto_detect.return_value = mock_provider

            result = manager.read("auto")

            mock_auto_detect.assert_called_once()
            mock_provider.get_token.assert_called_once()
            assert result == "auto detect result"

    def test_read_with_specific_type(self):
        """Test read method with specific provider type delegates to get_oidc_provider."""
        manager = WorkloadIdentityManager()

        with patch(
            "snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider"
        ) as mock_get_provider:
            mock_provider = Mock()
            mock_provider.provider_name = "github"
            mock_provider.get_token.return_value = "specific result"
            mock_get_provider.return_value = mock_provider

            result = manager.read(OidcProviderType.GITHUB.value)

            mock_get_provider.assert_called_once_with(OidcProviderType.GITHUB.value)
            mock_provider.get_token.assert_called_once()
            assert result == "specific result"

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.manager.auto_detect_oidc_provider"
    )
    def test_read_auto_detect_token_success(self, mock_auto_detect):
        """Test read with auto when provider is available and working."""
        # Create mock provider
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.return_value = "mock_token"

        mock_auto_detect.return_value = mock_provider

        manager = WorkloadIdentityManager()
        result = manager.read("auto")

        mock_auto_detect.assert_called_once()
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.manager.auto_detect_oidc_provider"
    )
    def test_read_auto_detect_token_success_no_info(self, mock_auto_detect):
        """Test read with auto when provider works but has no token info."""
        # Create mock provider with no token info
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.return_value = "mock_token"
        mock_provider.get_token_info.return_value = {}

        mock_auto_detect.return_value = mock_provider

        manager = WorkloadIdentityManager()
        result = manager.read("auto")

        mock_auto_detect.assert_called_once()
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.manager.auto_detect_oidc_provider"
    )
    def test_read_auto_detect_token_no_provider_with_available_providers(
        self, mock_auto_detect
    ):
        """Test read with auto when no provider detected but providers are registered."""
        mock_auto_detect.side_effect = CliError(
            "No OIDC provider detected in current environment. Available providers: github, other_provider. Use --type <provider> to specify a provider explicitly."
        )

        manager = WorkloadIdentityManager()

        with pytest.raises(
            CliError, match="No OIDC provider detected in current environment"
        ):
            manager.read("auto")

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.manager.auto_detect_oidc_provider"
    )
    def test_read_auto_detect_token_provider_fails(self, mock_auto_detect):
        """Test read with auto when provider fails to get token."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.side_effect = Exception("Token retrieval failed")

        mock_auto_detect.return_value = mock_provider

        manager = WorkloadIdentityManager()

        with pytest.raises(
            CliError,
            match=f"Failed to retrieve token from {OidcProviderType.GITHUB.value}: Token retrieval failed",
        ):
            manager.read("auto")

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider")
    def test_read_specific_token_success(self, mock_get_provider):
        """Test read with specific provider when provider exists and works."""
        # Create mock provider
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = True
        mock_provider.get_token.return_value = "mock_token"

        mock_get_provider.return_value = mock_provider

        manager = WorkloadIdentityManager()
        result = manager.read(OidcProviderType.GITHUB.value)

        mock_get_provider.assert_called_once_with(OidcProviderType.GITHUB.value)
        mock_provider.get_token.assert_called_once()
        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider")
    def test_read_specific_token_provider_not_found(self, mock_get_provider):
        """Test read with specific provider when provider doesn't exist."""
        mock_get_provider.side_effect = CliError(
            "Unknown provider 'unknown_provider'. Available providers: github, other_provider"
        )

        manager = WorkloadIdentityManager()

        with pytest.raises(CliError, match="Unknown provider 'unknown_provider'"):
            manager.read("unknown_provider")

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider")
    def test_read_specific_token_provider_not_available(self, mock_get_provider):
        """Test read with specific provider when provider exists but is not available."""
        mock_get_provider.side_effect = CliError(
            f"Provider '{OidcProviderType.GITHUB.value}' is not available in the current environment."
        )

        manager = WorkloadIdentityManager()

        with pytest.raises(
            CliError,
            match=f"Provider '{OidcProviderType.GITHUB.value}' is not available in the current environment",
        ):
            manager.read(OidcProviderType.GITHUB.value)

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider")
    def test_read_specific_token_provider_fails(self, mock_get_provider):
        """Test read with specific provider when provider exists but fails to get token."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = True
        mock_provider.get_token.side_effect = Exception("Token retrieval failed")

        mock_get_provider.return_value = mock_provider

        manager = WorkloadIdentityManager()

        with pytest.raises(
            CliError,
            match=f"Failed to retrieve token from {OidcProviderType.GITHUB.value}: Token retrieval failed",
        ):
            manager.read(OidcProviderType.GITHUB.value)

    def test_manager_inherits_from_sql_execution_mixin(self):
        """Test that WorkloadIdentityManager inherits from SqlExecutionMixin."""
        from snowflake.cli.api.sql_execution import SqlExecutionMixin

        manager = WorkloadIdentityManager()
        assert isinstance(manager, SqlExecutionMixin)

    def test_read_method_parameter_validation(self):
        """Test read method with different parameter types."""
        manager = WorkloadIdentityManager()

        # Test with empty string (should be treated as specific provider)
        with patch(
            "snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider"
        ) as mock_get_provider:
            mock_provider = Mock()
            mock_provider.provider_name = "empty"
            mock_provider.get_token.return_value = "empty result"
            mock_get_provider.return_value = mock_provider

            result = manager.read("")
            mock_get_provider.assert_called_once_with("")
            mock_provider.get_token.assert_called_once()
            assert result == "empty result"
