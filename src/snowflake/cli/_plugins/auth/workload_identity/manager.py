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

from snowflake.cli._app.auth.oidc_providers import (
    auto_detect_oidc_provider,
    get_oidc_provider,
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
            provider = auto_detect_oidc_provider()
        else:
            logger.debug("Using specific provider for OIDC token: %s", provider_type)
            provider = get_oidc_provider(provider_type)

        # Read token from provider with exception handling
        try:
            logger.debug("Retrieving token from provider: %s", provider.provider_name)
            token = provider.get_token()
            logger.info(
                "Successfully retrieved OIDC token from provider: %s",
                provider.provider_name,
            )
            return token
        except Exception as e:
            error_msg = "Failed to retrieve token from %s: %s" % (
                provider.provider_name,
                str(e),
            )
            logger.error(error_msg)
            raise CliError(error_msg)
