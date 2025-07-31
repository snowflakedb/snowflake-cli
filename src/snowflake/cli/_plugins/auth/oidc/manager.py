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
from functools import cached_property

from snowflake.cli._app.auth.oidc_providers import (
    OidcProviderError,
    auto_detect_oidc_provider,
    get_active_oidc_provider,
    get_oidc_provider,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

logger = logging.getLogger(__name__)


class OidcManager(SqlExecutionMixin):
    """
    Manages OIDC federated authentication.

    This class provides methods to set up, delete, and read OIDC federated
    configurations for authentication.
    """

    def setup(
        self,
        user: str,
        subject: str,
        default_role: str,
        provider_type: str,
    ) -> str:
        """
        Sets up OIDC federated authentication for the specified user.

        Args:
            user: Name for the federated user to create
            subject: OIDC subject string
            default_role: Default role to assign to the federated user
            provider_type: Type of OIDC provider to use

        Returns:
            Success message string

        Raises:
            CliError: If user creation fails or parameters are invalid
        """
        logger.info(
            "Setting up OIDC federated authentication for user: %s with provider type: %s",
            user,
            provider_type,
        )

        if not subject.strip():
            raise CliError("Subject cannot be empty")

        # Get issuer from the specified provider
        try:
            provider = get_oidc_provider(provider_type)
            if provider is None:
                raise CliError("Provider '%s' is not available" % provider_type)

            issuer = provider.issuer
        except OidcProviderError as e:
            error_msg = "OIDC provider configuration error: %s" % str(e)
            logger.error(error_msg)
            raise CliError(error_msg)
        except Exception as e:
            error_msg = "Failed to get OIDC provider '%s': %s" % (provider_type, str(e))
            logger.error(error_msg)
            raise CliError(error_msg)

        # Construct the CREATE USER SQL command using WORKLOAD_IDENTITY syntax
        logger.debug("Using WORKLOAD_IDENTITY syntax for user creation")
        create_user_sql = (
            f"CREATE USER {user} WORKLOAD_IDENTITY = ("
            f" TYPE = 'OIDC'"
            f" ISSUER = '{issuer}'"
            f" SUBJECT = '{subject}')"
            f" TYPE = SERVICE DEFAULT_ROLE = {default_role}"
        )

        try:
            logger.debug("Executing CREATE USER command for federated user: %s", user)
            self.execute_query(create_user_sql)

            success_message = (
                "Successfully created federated user '%s' with subject '%s' using provider '%s'"
                % (user, subject, provider_type)
            )
            logger.info(success_message)
            return success_message
        except Exception as e:
            error_msg = "Failed to create federated user '%s': %s" % (user, str(e))
            logger.error(error_msg)
            raise CliError(error_msg)

    def delete(self, user: str) -> str:
        """
        Deletes a federated user.

        Args:
            user: Name of the federated user to delete

        Returns:
            Success message string

        Raises:
            CliError: If user deletion fails or parameters are invalid
        """
        logger.info("Deleting federated user: %s", user)

        if not user.strip():
            raise CliError("Federated user name cannot be empty")

        # Basic validation for user name format
        if user.strip() and (
            user[0].isdigit() or not user.replace("_", "").replace("-", "").isalnum()
        ):
            raise CliError("Invalid federated user name")

        try:
            logger.debug("Executing DROP USER command for federated user: %s", user)
            self.execute_query(f"DROP USER {user}")

            success_message = "Successfully deleted federated user '%s'" % user
            logger.info(success_message)
            return success_message
        except Exception as e:
            error_msg = "Failed to delete federated user '%s': %s" % (user, str(e))
            logger.error(error_msg)
            raise CliError(error_msg)

    def read(self, provider_type: str = "auto") -> str:
        """
        Reads OIDC token based on the specified provider type.

        Args:
            provider_type: Type of provider to read token from ("auto" for auto-detection)

        Returns:
            Token string or provider information

        Raises:
            CliError: If token reading fails
        """
        logger.info("Reading OIDC token with provider type: %s", provider_type)

        try:
            if provider_type == "auto":
                provider = auto_detect_oidc_provider()
                if provider is None:
                    raise CliError("No available OIDC provider found")
                return provider.get_token()
            else:
                provider = get_active_oidc_provider(provider_type)
                if provider is None:
                    raise CliError(f"Provider '{provider_type}' is not available")
                return provider.get_token()
        except OidcProviderError as e:
            error_msg = "OIDC provider error: %s" % str(e)
            logger.error(error_msg)
            raise CliError(error_msg)
        except Exception as e:
            error_msg = "Failed to read OIDC token: %s" % str(e)
            logger.error(error_msg)
            raise CliError(error_msg)

    @cached_property
    def _has_oidc_enabled(self) -> bool:
        """
        Checks if OIDC federated authentication is enabled in the Snowflake account.
        """
        logger.debug("Checking ENABLE_USERS_HAS_WORKLOAD_IDENTITY parameter")
        parameter_result = self.execute_query(
            'show parameters ->> select "key", "value" from $1 where "key" = \'ENABLE_USERS_HAS_WORKLOAD_IDENTITY\'',
            cursor_class=DictCursor,
        ).fetchone()
        return parameter_result and parameter_result.get("value", "").lower() == "true"

    def get_users_list(self) -> SnowflakeCursor:
        """
        Lists users with OIDC federated authentication enabled.

        Returns:
            List of users with OIDC federated authentication enabled

        Raises:
            CliError: If queries fail or parameters are invalid
        """
        logger.info("Listing users with OIDC federated authentication enabled")

        try:
            # Determine which column to check based on parameter value
            # TODO: This is a temporary solution to check if OIDC is enabled. SNOW-2236350
            if self._has_oidc_enabled:
                logger.debug("Using has_workload_identity column")
                users_query = 'show terse users ->> select * from $1 where "has_workload_identity" = true'
            else:
                logger.debug("Using has_federated_workload_authentication column")
                users_query = 'show terse users ->> select * from $1 where "has_federated_workload_authentication" = true'

            # Execute the users query
            users_result = self.execute_query(users_query, cursor_class=DictCursor)

            logger.info(
                "Found %d users with OIDC federated authentication enabled",
                users_result.rowcount,
            )
            return users_result

        except Exception as e:
            error_msg = (
                "Failed to list users with OIDC federated authentication: %s" % str(e)
            )
            logger.error(error_msg)
            raise CliError(error_msg)
