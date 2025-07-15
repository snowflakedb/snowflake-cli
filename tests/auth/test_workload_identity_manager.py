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
        """Test read method with 'auto' type delegates to _read_auto_detect_token."""
        manager = WorkloadIdentityManager()

        with patch.object(
            manager, "_read_auto_detect_token", return_value="auto detect result"
        ) as mock_auto:
            result = manager.read("auto")

            mock_auto.assert_called_once()
            assert result == "auto detect result"

    def test_read_with_specific_type(self):
        """Test read method with specific provider type delegates to _read_specific_token."""
        manager = WorkloadIdentityManager()

        with patch.object(
            manager, "_read_specific_token", return_value="specific result"
        ) as mock_specific:
            result = manager.read(OidcProviderType.GITHUB.value)

            mock_specific.assert_called_once_with(OidcProviderType.GITHUB.value)
            assert result == "specific result"

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.manager.auto_detect_oidc_provider"
    )
    def test_read_auto_detect_token_success(self, mock_auto_detect):
        """Test _read_auto_detect_token when provider is available and working."""
        # Create mock provider
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.return_value = "mock_token"
        mock_provider.get_token_info.return_value = {
            "issuer": "https://token.actions.githubusercontent.com",
            "provider": OidcProviderType.GITHUB.value,
        }

        mock_auto_detect.return_value = mock_provider

        manager = WorkloadIdentityManager()
        result = manager._read_auto_detect_token()  # noqa: SLF001

        mock_auto_detect.assert_called_once()
        mock_provider.get_token.assert_called_once()
        mock_provider.get_token_info.assert_called_once()

        assert result == "mock_token"

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.manager.auto_detect_oidc_provider"
    )
    def test_read_auto_detect_token_success_no_info(self, mock_auto_detect):
        """Test _read_auto_detect_token when provider works but has no token info."""
        # Create mock provider with no token info
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.return_value = "mock_token"
        mock_provider.get_token_info.return_value = {}

        mock_auto_detect.return_value = mock_provider

        manager = WorkloadIdentityManager()
        result = manager._read_auto_detect_token()  # noqa: SLF001

        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.list_oidc_providers")
    @patch(
        "snowflake.cli._plugins.auth.workload_identity.manager.auto_detect_oidc_provider"
    )
    def test_read_auto_detect_token_no_provider_with_available_providers(
        self, mock_auto_detect, mock_list_providers
    ):
        """Test _read_auto_detect_token when no provider detected but providers are registered."""
        mock_auto_detect.return_value = None
        mock_list_providers.return_value = [
            OidcProviderType.GITHUB.value,
            "other_provider",
        ]

        manager = WorkloadIdentityManager()

        with pytest.raises(
            CliError, match="No OIDC provider detected in current environment"
        ):
            manager._read_auto_detect_token()  # noqa: SLF001

        mock_auto_detect.assert_called_once()
        mock_list_providers.assert_called_once()

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.list_oidc_providers")
    @patch(
        "snowflake.cli._plugins.auth.workload_identity.manager.auto_detect_oidc_provider"
    )
    def test_read_auto_detect_token_no_provider_no_providers_registered(
        self, mock_auto_detect, mock_list_providers
    ):
        """Test _read_auto_detect_token when no provider detected and no providers registered."""
        mock_auto_detect.return_value = None
        mock_list_providers.return_value = []

        manager = WorkloadIdentityManager()

        with pytest.raises(CliError, match="No OIDC providers are registered"):
            manager._read_auto_detect_token()  # noqa: SLF001

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.manager.auto_detect_oidc_provider"
    )
    def test_read_auto_detect_token_provider_fails(self, mock_auto_detect):
        """Test _read_auto_detect_token when provider fails to get token."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.side_effect = Exception("Token retrieval failed")

        mock_auto_detect.return_value = mock_provider

        manager = WorkloadIdentityManager()

        with pytest.raises(
            CliError,
            match=f"Failed to retrieve token from {OidcProviderType.GITHUB.value}: Token retrieval failed",
        ):
            manager._read_auto_detect_token()  # noqa: SLF001

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider")
    def test_read_specific_token_success(self, mock_get_provider):
        """Test _read_specific_token when provider exists and works."""
        # Create mock provider
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = True
        mock_provider.get_token.return_value = "mock_token"
        mock_provider.get_token_info.return_value = {
            "issuer": "https://token.actions.githubusercontent.com",
            "provider": OidcProviderType.GITHUB.value,
        }

        mock_get_provider.return_value = mock_provider

        manager = WorkloadIdentityManager()
        result = manager._read_specific_token(  # noqa: SLF001
            OidcProviderType.GITHUB.value
        )  # noqa: SLF001

        mock_get_provider.assert_called_once_with(OidcProviderType.GITHUB.value)
        mock_provider.get_token.assert_called_once()
        mock_provider.get_token_info.assert_called_once()

        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider")
    def test_read_specific_token_success_no_info(self, mock_get_provider):
        """Test _read_specific_token when provider works but has no token info."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = True
        mock_provider.get_token.return_value = "mock_token"
        mock_provider.get_token_info.return_value = {}

        mock_get_provider.return_value = mock_provider

        manager = WorkloadIdentityManager()
        result = manager._read_specific_token(  # noqa: SLF001
            OidcProviderType.GITHUB.value
        )  # noqa: SLF001

        assert result == "mock_token"

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.list_oidc_providers")
    @patch("snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider")
    def test_read_specific_token_provider_not_found(
        self, mock_get_provider, mock_list_providers
    ):
        """Test _read_specific_token when provider doesn't exist."""
        mock_get_provider.return_value = None
        mock_list_providers.return_value = [
            OidcProviderType.GITHUB.value,
            "other_provider",
        ]

        manager = WorkloadIdentityManager()

        with pytest.raises(CliError, match="Unknown provider 'unknown_provider'"):
            manager._read_specific_token("unknown_provider")  # noqa: SLF001

        mock_get_provider.assert_called_once_with("unknown_provider")
        mock_list_providers.assert_called_once()

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider")
    def test_read_specific_token_provider_not_available(self, mock_get_provider):
        """Test _read_specific_token when provider exists but is not available."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = False

        mock_get_provider.return_value = mock_provider

        manager = WorkloadIdentityManager()

        with pytest.raises(
            CliError,
            match=f"Provider '{OidcProviderType.GITHUB.value}' is not available in the current environment",
        ):
            manager._read_specific_token(OidcProviderType.GITHUB.value)  # noqa: SLF001

        mock_get_provider.assert_called_once_with(OidcProviderType.GITHUB.value)

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider")
    def test_read_specific_token_provider_fails(self, mock_get_provider):
        """Test _read_specific_token when provider exists but fails to get token."""
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
            manager._read_specific_token(OidcProviderType.GITHUB.value)  # noqa: SLF001

    def test_manager_inherits_from_sql_execution_mixin(self):
        """Test that WorkloadIdentityManager inherits from SqlExecutionMixin."""
        from snowflake.cli.api.sql_execution import SqlExecutionMixin

        manager = WorkloadIdentityManager()
        assert isinstance(manager, SqlExecutionMixin)

    def test_read_method_parameter_validation(self):
        """Test read method with different parameter types."""
        manager = WorkloadIdentityManager()

        # Test with empty string (should be treated as specific provider)
        with patch.object(
            manager, "_read_specific_token", return_value="empty result"
        ) as mock_specific:
            result = manager.read("")
            mock_specific.assert_called_once_with("")
            assert result == "empty result"

    @patch(
        "snowflake.cli._plugins.auth.workload_identity.manager.auto_detect_oidc_provider"
    )
    def test_read_auto_detect_token_provider_get_token_info_fails(
        self, mock_auto_detect
    ):
        """Test _read_auto_detect_token when get_token_info fails but get_token succeeds."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.get_token.return_value = "mock_token"
        mock_provider.get_token_info.side_effect = Exception("Info retrieval failed")

        mock_auto_detect.return_value = mock_provider

        manager = WorkloadIdentityManager()

        # The method should still fail because the exception is caught in the outer try/except
        with pytest.raises(
            CliError,
            match=f"Failed to retrieve token from {OidcProviderType.GITHUB.value}: Info retrieval failed",
        ):
            manager._read_auto_detect_token()  # noqa: SLF001

    @patch("snowflake.cli._plugins.auth.workload_identity.manager.get_oidc_provider")
    def test_read_specific_token_provider_get_token_info_fails(self, mock_get_provider):
        """Test _read_specific_token when get_token_info fails but get_token succeeds."""
        mock_provider = Mock()
        mock_provider.provider_name = OidcProviderType.GITHUB.value
        mock_provider.is_available = True
        mock_provider.get_token.return_value = "mock_token"
        mock_provider.get_token_info.side_effect = Exception("Info retrieval failed")

        mock_get_provider.return_value = mock_provider

        manager = WorkloadIdentityManager()

        # The method should still fail because the exception is caught in the outer try/except
        with pytest.raises(
            CliError,
            match=f"Failed to retrieve token from {OidcProviderType.GITHUB.value}: Info retrieval failed",
        ):
            manager._read_specific_token(OidcProviderType.GITHUB.value)  # noqa: SLF001
