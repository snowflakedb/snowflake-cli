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

import logging

from snowflake.cli._plugins.auth.workload_identity.oidc_providers import (
    auto_detect_oidc_provider,
    get_oidc_provider,
    list_oidc_providers,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sql_execution import SqlExecutionMixin

logger = logging.getLogger(__name__)


class WorkloadIdentityManager(SqlExecutionMixin):
    """
    Manager for GitHub workload identity federation authentication.
    """

    def setup(self, github_repository: str) -> str:
        """
        Sets up GitHub workload identity federation for the specified repository.

        Args:
            github_repository: GitHub repository in format 'owner/repo'

        Returns:
            Success message string

        Raises:
            NotImplementedError: Setup functionality is not yet implemented
        """
        logger.info(
            "Attempting to set up GitHub workload identity federation for repository: %s",
            github_repository,
        )
        logger.warning(
            "GitHub workload identity federation setup is not yet implemented"
        )
        raise NotImplementedError(
            "GitHub workload identity federation setup is not yet implemented"
        )

    def read(self, provider_type: str) -> str:
        """
        Reads OIDC token based on the specified type.

        Args:
            provider_type: Type of provider ('auto' for auto-detection or specific provider name)

        Returns:
            The OIDC token string

        Raises:
            CliError: If token cannot be retrieved or provider is not available
        """
        logger.info("Reading OIDC token with provider type: %s", provider_type)

        if provider_type == "auto":
            logger.debug("Using auto-detection for OIDC token")
            return self._read_auto_detect_token()
        else:
            logger.debug("Using specific provider for OIDC token: %s", provider_type)
            return self._read_specific_token(provider_type)

    def _read_auto_detect_token(self) -> str:
        """
        Auto-detects and reads OIDC token from available providers.

        Returns:
            The OIDC token string

        Raises:
            CliError: If no providers are available or token cannot be retrieved
        """
        logger.debug("Starting auto-detection of OIDC providers")
        provider = auto_detect_oidc_provider()

        if not provider:
            logger.warning("No OIDC provider detected in current environment")
            available_providers = list_oidc_providers()
            logger.debug("Available providers: %s", available_providers)

            if available_providers:
                providers_list = ", ".join(available_providers)
                error_msg = (
                    "No OIDC provider detected in current environment. "
                    "Available providers: %s. "
                    "Use --type <provider> to specify a provider explicitly."
                ) % providers_list
                logger.error(error_msg)
                raise CliError(error_msg)
            else:
                error_msg = "No OIDC providers are registered."
                logger.error(error_msg)
                raise CliError(error_msg)

        logger.info("Auto-detected OIDC provider: %s", provider.provider_name)

        try:
            logger.debug("Retrieving token from provider: %s", provider.provider_name)
            token = provider.get_token()

            logger.debug(
                "Retrieving token info from provider: %s", provider.provider_name
            )
            token_info = provider.get_token_info()

            info_str = "Provider: %s" % provider.provider_name
            if token_info:
                info_details = ", ".join(
                    ["%s: %s" % (k, v) for k, v in token_info.items()]
                )
                info_str += " (%s)" % info_details

            logger.info(
                "Successfully retrieved OIDC token via auto-detection: %s", info_str
            )
            return token
        except Exception as e:
            error_msg = "Failed to retrieve token from %s: %s" % (
                provider.provider_name,
                str(e),
            )
            logger.error(error_msg)
            raise CliError(error_msg)

    def _read_specific_token(self, provider_name: str) -> str:
        """
        Reads OIDC token from a specific provider.

        Args:
            provider_name: Name of the provider to use

        Returns:
            The OIDC token string

        Raises:
            CliError: If provider is unknown, unavailable, or token cannot be retrieved
        """
        logger.debug("Reading OIDC token from specific provider: %s", provider_name)
        provider = get_oidc_provider(provider_name)

        if not provider:
            logger.warning("Provider '%s' not found", provider_name)
            available_providers = list_oidc_providers()
            logger.debug("Available providers: %s", available_providers)
            providers_list = ", ".join(available_providers)
            error_msg = "Unknown provider '%s'. Available providers: %s" % (
                provider_name,
                providers_list,
            )
            logger.error(error_msg)
            raise CliError(error_msg)

        logger.debug("Checking availability of provider: %s", provider_name)
        if not provider.is_available:
            error_msg = (
                "Provider '%s' is not available in the current environment."
                % provider_name
            )
            logger.error(error_msg)
            raise CliError(error_msg)

        logger.info("Using provider: %s", provider_name)

        try:
            logger.debug("Retrieving token from provider: %s", provider_name)
            token = provider.get_token()

            logger.debug("Retrieving token info from provider: %s", provider_name)
            token_info = provider.get_token_info()

            info_str = "Provider: %s" % provider.provider_name
            if token_info:
                info_details = ", ".join(
                    ["%s: %s" % (k, v) for k, v in token_info.items()]
                )
                info_str += " (%s)" % info_details

            logger.info(
                "Successfully retrieved OIDC token from %s: %s", provider_name, info_str
            )
            return token
        except Exception as e:
            error_msg = "Failed to retrieve token from %s: %s" % (provider_name, str(e))
            logger.error(error_msg)
            raise CliError(error_msg)
