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
from textwrap import dedent
from typing import TypeAlias

from snowflake.cli._app.auth.errors import OidcProviderError
from snowflake.cli._app.auth.oidc_providers import (
    OidcProviderType,
    OidcProviderTypeWithAuto,
    auto_detect_oidc_provider,
    get_active_oidc_provider,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sql_execution import SqlExecutionMixin

logger = logging.getLogger(__name__)


Providers: TypeAlias = OidcProviderType | OidcProviderTypeWithAuto


class OidcManager(SqlExecutionMixin):
    """
    Manages OIDC authentication.

    This class provides methods to set up, delete, and read OIDC
    configurations for authentication.
    """

    def create_user(
        self,
        *,
        user_name: str,
        issuer: str,
        subject: str,
        default_role: str,
    ) -> str:
        """
        Sets up OIDC authentication for the specified user.

        Args:
            user_name: Name for the user to create
            subject: OIDC subject string
            default_role: Default role to assign to the user
            issuer: OIDC issuer URL

        Returns:
            Success message string

        Raises:
            CliError: If user creation fails or parameters are invalid
        """
        logger.info(
            (
                "Setting up OIDC authentication for user: %r "
                "with issuer: %r, subject: %r and default_role: %r"
            ),
            user_name,
            issuer,
            subject,
            default_role,
        )

        create_user_sql = (
            f"CREATE USER {user_name} WORKLOAD_IDENTITY = ("
            f" TYPE = 'OIDC'"
            f" ISSUER = '{issuer}'"
            f" SUBJECT = '{subject}')"
            f" TYPE = SERVICE"
        )
        if default_role:
            create_user_sql = f"{create_user_sql} DEFAULT_ROLE = {default_role}"

        try:
            logger.debug("Executing CREATE USER command for user: %s", user_name)
            self.execute_query(create_user_sql)

            success_message = (
                "Successfully created OIDC user '%s' with subject '%s' and issuer '%s'"
                % (user_name, subject, issuer)
            )
            logger.info(success_message)
            return success_message
        except Exception as e:
            error_msg = "Failed to create user '%s': %s" % (
                user_name,
                str(e),
            )
            logger.error(error_msg)
            raise CliError(error_msg)

    def delete(self, user: str) -> str:
        """
        Deletes a user.

        Args:
            user: Name of the user to delete

        Returns:
            Success message string

        Raises:
            CliError: If user deletion fails or parameters are invalid
        """
        logger.info("Deleting user: %r", user)

        _user = user.strip()
        if not _user:
            raise CliError("User name cannot be empty")

        logger.debug("Searching for user %r", _user)

        _auth_types = dedent(
            f"""
            show user workload identity authentication methods for user {_user} ->>
            select
                "name",
                "type"
            from $1
            where
                "type" = 'OIDC'
        """
        )
        logger.debug("Search statement: %r", _auth_types)

        _search_res = self.execute_query(_auth_types).fetchall()
        logger.debug("Search results: %r", _search_res)

        _search_count = len(_search_res)
        match _search_count:
            case 1:
                logger.debug("Executing DROP USER command for user: %r", _user)
                self.execute_query(f'DROP USER "{_user}"')
                success_message = f"Successfully deleted user {_user!r}"
                logger.info(success_message)
                return success_message
            case 0:
                msg = f"User {_user!r} not found"
                logger.debug(msg)
                raise CliError(msg)
            case _:
                msg = f"Error searching for user {_user!r}"
                logger.debug(msg)
                raise CliError(msg)

    def read_token(
        self,
        provider_type: Providers = OidcProviderTypeWithAuto.AUTO,
    ) -> str:
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
            if provider_type == OidcProviderTypeWithAuto.AUTO:
                provider = auto_detect_oidc_provider()
            else:
                provider = get_active_oidc_provider(provider_type.value)
            return provider.get_token()
        except OidcProviderError as e:
            logger.error("OIDC provider error: %s", str(e))
            raise CliError(str(e))

    @property
    def _workload_identity_enabled_users_stmt(self) -> str:
        """SQL statement for listing users with workload identity enabled."""
        return dedent(
            """
            show terse users ->>
            select "name", "created_on", "display_name", "type", "has_workload_identity"
            from $1
            where "has_workload_identity" = true
        """
        )
