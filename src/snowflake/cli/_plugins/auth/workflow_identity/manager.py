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


from snowflake.cli._plugins.auth.workflow_identity.oidc_providers import (
    auto_detect_oidc_provider,
    get_oidc_provider,
    list_oidc_providers,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sql_execution import SqlExecutionMixin


class WorkflowIdentityManager(SqlExecutionMixin):
    """
    Manager for GitHub workflow identity federation authentication.
    """

    def setup(self, github_repository: str) -> str:
        """
        Sets up GitHub workflow identity federation for the specified repository.

        Args:
            github_repository: GitHub repository in format 'owner/repo'

        Returns:
            Success message string

        Raises:
            NotImplementedError: Setup functionality is not yet implemented
        """
        raise NotImplementedError(
            "GitHub workflow identity federation setup is not yet implemented"
        )

    def read(self, provider_type: str) -> str:
        """
        Reads OIDC token based on the specified type.

        Args:
            provider_type: Type of provider ('auto' for auto-detection or specific provider name)

        Returns:
            Success message with token information

        Raises:
            CliError: If token cannot be retrieved or provider is not available
        """
        if provider_type == "auto":
            return self._read_auto_detect_token()
        else:
            return self._read_specific_token(provider_type)

    def _read_auto_detect_token(self) -> str:
        """
        Auto-detects and reads OIDC token from available providers.

        Returns:
            Success message with token information

        Raises:
            CliError: If no providers are available or token cannot be retrieved
        """
        provider = auto_detect_oidc_provider()
        if not provider:
            available_providers = list_oidc_providers()
            if available_providers:
                providers_list = ", ".join(available_providers)
                raise CliError(
                    f"No OIDC provider detected in current environment. "
                    f"Available providers: {providers_list}. "
                    f"Use --type <provider> to specify a provider explicitly."
                )
            else:
                raise CliError("No OIDC providers are registered.")

        try:
            token = provider.get_token()
            token_info = provider.get_token_info()

            info_str = f"Provider: {provider.provider_name}"
            if token_info:
                info_details = ", ".join([f"{k}: {v}" for k, v in token_info.items()])
                info_str += f" ({info_details})"

            return f"OIDC token detected. {info_str}"
        except Exception as e:
            raise CliError(
                f"Failed to retrieve token from {provider.provider_name}: {str(e)}"
            )

    def _read_specific_token(self, provider_name: str) -> str:
        """
        Reads OIDC token from a specific provider.

        Args:
            provider_name: Name of the provider to use

        Returns:
            Success message with token information

        Raises:
            CliError: If provider is unknown, unavailable, or token cannot be retrieved
        """
        provider = get_oidc_provider(provider_name)
        if not provider:
            available_providers = list_oidc_providers()
            providers_list = ", ".join(available_providers)
            raise CliError(
                f"Unknown provider '{provider_name}'. "
                f"Available providers: {providers_list}"
            )

        if not provider.is_available:
            raise CliError(
                f"Provider '{provider_name}' is not available in the current environment."
            )

        try:
            token = provider.get_token()
            token_info = provider.get_token_info()

            info_str = f"Provider: {provider.provider_name}"
            if token_info:
                info_details = ", ".join([f"{k}: {v}" for k, v in token_info.items()])
                info_str += f" ({info_details})"

            return f"OIDC token retrieved. {info_str}"
        except Exception as e:
            raise CliError(f"Failed to retrieve token from {provider_name}: {str(e)}")
