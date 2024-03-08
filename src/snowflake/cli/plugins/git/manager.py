from textwrap import dedent

from snowflake.cli.plugins.object.stage.manager import StageManager
from snowflake.connector.cursor import SnowflakeCursor


class GitManager(StageManager):
    def show_branches(self, repo_name: str, like: str) -> SnowflakeCursor:
        query = f"show git branches like '{like}' in {repo_name}"
        return self._execute_query(query)

    def show_tags(self, repo_name: str, like: str) -> SnowflakeCursor:
        query = f"show git tags like '{like}' in {repo_name}"
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
        query = dedent(
            f"""
            create git repository {repo_name}
            api_integration = {api_integration}
            origin = '{url}'
            """
        )
        if secret is not None:
            query += f"git_credentials = {secret}\n"
        return self._execute_query(query)
