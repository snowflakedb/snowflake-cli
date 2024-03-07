from typing import Optional

from snowflake.cli.plugins.object.stage.manager import StageManager
from snowflake.connector.cursor import SnowflakeCursor


class GitManager(StageManager):
    def show_branches(self, repo_name: str) -> SnowflakeCursor:
        query = f"show git branches in {repo_name}"
        return self._execute_query(query)

    def show_tags(self, repo_name: str) -> SnowflakeCursor:
        query = f"show git tags in {repo_name}"
        return self._execute_query(query)

    def show_files(self, repo_path: str) -> SnowflakeCursor:
        query = f"ls {repo_path}"
        return self._execute_query(query)

    def fetch(self, repo_name: str) -> SnowflakeCursor:
        query = f"alter git repository {repo_name} fetch"
        return self._execute_query(query)

    def create(
        self, repo_name: str, api_integration: str, url: str, secret: str
    ) -> SnowflakeCursor:
        query = (
            f"create git repository {repo_name}"
            f" api_integration = {api_integration}"
            f" origin = '{url}'"
        )
        if secret is not None:
            query += f" git_credentials = {secret}"
        return self._execute_query(query)

    def create_secret(self, name: str, username: str, password: str) -> SnowflakeCursor:
        query = (
            f"create secret {name}"
            f" type = password"
            f" username = '{username}'"
            f" password = '{password}'"
        )
        return self._execute_query(query)

    def create_api_integration(
        self, name: str, allowed_prefix: str, secret: Optional[str]
    ) -> SnowflakeCursor:
        query = (
            f"create api integration {name}"
            f" api_provider = git_https_api"
            f" api_allowed_prefixes = ('{allowed_prefix}')"
            f" allowed_authentication_secrets = ({secret if secret else ''})"
            f" enabled = true"
        )
        return self._execute_query(query)
