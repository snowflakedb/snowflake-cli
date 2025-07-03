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

from typing import Optional

from click import ClickException
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class WorkflowIdentityManager(SqlExecutionMixin):
    """
    Manager for GitHub workflow identity federation authentication.
    """

    def setup(self, github_repository: str) -> None:
        """
        Sets up GitHub workflow identity federation for the specified repository.

        Args:
            github_repository: GitHub repository in format 'owner/repo'
        """
        if not self._validate_github_repository_format(github_repository):
            raise ClickException(
                "Invalid GitHub repository format. Expected format: 'owner/repo'"
            )

        cli_context = get_cli_context()
        user = cli_context.connection.user

        # Create or update the security integration for GitHub workflow identity federation
        self._create_security_integration(github_repository)
        cli_console.step(f"Created security integration for {github_repository}")

        # Set the workflow identity property for the user
        self._set_workflow_identity_property(user, github_repository)
        cli_console.step(f"Set workflow identity property for user {user}")

    def status(self) -> str:
        """
        Returns the status of GitHub workflow identity federation configuration.

        Returns:
            Status message indicating if workflow identity federation is configured
        """
        cli_context = get_cli_context()
        user = cli_context.connection.user

        try:
            # Check if workflow identity is configured for the user
            workflow_identity = self._get_workflow_identity_property(user)
            if workflow_identity:
                return f"Configured for repository: {workflow_identity}"
            else:
                return "Not configured"
        except Exception as e:
            return f"Error checking status: {str(e)}"

    def remove(self) -> SnowflakeCursor:
        """
        Removes the GitHub workflow identity federation configuration.

        Returns:
            SnowflakeCursor with the result of the operation
        """
        cli_context = get_cli_context()
        user = cli_context.connection.user

        return self.execute_query(f"ALTER USER {user} UNSET GITHUB_WORKFLOW_IDENTITY")

    def _validate_github_repository_format(self, repository: str) -> bool:
        """
        Validates that the GitHub repository is in the correct format (owner/repo).

        Args:
            repository: The repository string to validate

        Returns:
            True if format is valid, False otherwise
        """
        parts = repository.split("/")
        return len(parts) == 2 and all(part.strip() for part in parts)

    def _create_security_integration(self, github_repository: str) -> SnowflakeCursor:
        """
        Creates or replaces a security integration for GitHub workflow identity federation.

        Args:
            github_repository: GitHub repository in format 'owner/repo'

        Returns:
            SnowflakeCursor with the result of the operation
        """
        integration_name = f"GITHUB_WIF_{github_repository.replace('/', '_').upper()}"

        sql = f"""
        CREATE OR REPLACE SECURITY INTEGRATION {integration_name}
        TYPE = OAUTH
        OAUTH_CLIENT = GITHUB_ACTIONS
        OAUTH_ALLOWED_AUTHORIZATION_ENDPOINTS = ('https://token.actions.githubusercontent.com')
        OAUTH_ALLOWED_TOKEN_ENDPOINTS = ('https://token.actions.githubusercontent.com')
        OAUTH_ALLOWED_SCOPES = ('repository:read')
        OAUTH_ISSUER = 'https://token.actions.githubusercontent.com'
        OAUTH_AUDIENCE = '{github_repository}'
        """

        return self.execute_query(sql)

    def _set_workflow_identity_property(
        self, user: str, github_repository: str
    ) -> SnowflakeCursor:
        """
        Sets the GitHub workflow identity property for a user.

        Args:
            user: The Snowflake user name
            github_repository: GitHub repository in format 'owner/repo'

        Returns:
            SnowflakeCursor with the result of the operation
        """
        return self.execute_query(
            f"ALTER USER {user} SET GITHUB_WORKFLOW_IDENTITY = '{github_repository}'"
        )

    def _get_workflow_identity_property(self, user: str) -> Optional[str]:
        """
        Gets the GitHub workflow identity property for a user.

        Args:
            user: The Snowflake user name

        Returns:
            The workflow identity property value if set, None otherwise
        """
        cursor = self.execute_query(f"DESCRIBE USER {user}")
        properties = cursor.fetchall()

        for prop in properties:
            if prop[0] == "GITHUB_WORKFLOW_IDENTITY" and prop[1]:
                return prop[1]

        return None
