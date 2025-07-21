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
    Manages workload identity federation for authentication.

    This class provides methods to set up, delete, and read workload identity
    configurations for federated authentication.
    """

    def setup(
        self,
        user: str,
        subject: str,
        default_role: str,
        provider_type: str,
    ) -> str:
        """
        Sets up workload identity federation for the specified user.

        Args:
            user: Name for the federated user to create
            subject: OIDC subject string
            default_role: Default role to assign to the federated user
            provider_type: Type of OIDC provider to use for issuer

        Returns:
            Success message string

        Raises:
            CliError: If user creation fails or parameters are invalid
        """
        logger.info(
            "Setting up workload identity federation for user: %s",
            user,
        )

        # Validate user name, subject, and role
        self._validate_user_name(user)
        self._validate_role_name(default_role)

        if not subject.strip():
            raise CliError("Subject cannot be empty")

        # Get issuer from the specified provider
        try:
            provider = get_oidc_provider(provider_type)
            issuer = provider.issuer
        except Exception as e:
            error_msg = f"Failed to get provider '{provider_type}': {str(e)}"
            logger.error(error_msg)
            raise CliError(error_msg)

        # Construct the CREATE USER SQL command
        create_user_sql = f"""CREATE USER {user}
  WORKLOAD_IDENTITY = (
    TYPE = 'OIDC'
    ISSUER = '{issuer}'
    SUBJECT = '{subject}'
  )
  TYPE = SERVICE
  DEFAULT_ROLE = {default_role}"""

        try:
            logger.debug("Executing CREATE USER command for federated user: %s", user)
            self.execute_query(create_user_sql)

            success_message = (
                f"Successfully created federated user '{user}' with subject '{subject}'"
            )
            logger.info(success_message)
            return success_message
        except Exception as e:
            error_msg = f"Failed to create federated user '{user}': {str(e)}"
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

        # Validate user name
        self._validate_user_name(user)

        try:
            logger.debug("Executing DROP USER command for federated user: %s", user)
            self.execute_query(f"DROP USER {user}")

            success_message = f"Successfully deleted federated user '{user}'"
            logger.info(success_message)
            return success_message
        except Exception as e:
            error_msg = f"Failed to delete federated user '{user}': {str(e)}"
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
                provider = get_oidc_provider(provider_type)
                if provider is None:
                    raise CliError(f"Provider '{provider_type}' is not available")
                return provider.get_token()
        except Exception as e:
            error_msg = f"Failed to read OIDC token: {str(e)}"
            logger.error(error_msg)
            raise CliError(error_msg)

    def _validate_user_name(self, user_name: str) -> None:
        """
        Validates the federated user name.

        Args:
            user_name: The user name to validate

        Raises:
            CliError: If the user name is invalid
        """
        if not user_name or not user_name.strip():
            raise CliError("Federated user name cannot be empty")

        # Check if user name starts with a digit (basic SQL identifier validation)
        if user_name[0].isdigit():
            raise CliError("Invalid federated user name: cannot start with a digit")

    def _validate_role_name(self, role_name: str) -> None:
        """
        Validates the role name.

        Args:
            role_name: The role name to validate

        Raises:
            CliError: If the role name is invalid
        """
        if not role_name or not role_name.strip():
            raise CliError("Default role name cannot be empty")
