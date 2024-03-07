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
