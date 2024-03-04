from pathlib import Path

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

    def _get_standard_stage_directory_path(self, path):
        if not path.endswith("/"):
            path += "/"
        return self.get_standard_stage_name(path)

    def get(self, repo_path: str, target_path: Path, parallel: int) -> SnowflakeCursor:
        return super().get(
            stage_name=self._get_standard_stage_directory_path(repo_path),
            dest_path=target_path,
            parallel=parallel,
        )

    def copy(self, repo_path: str, destination_path: str) -> SnowflakeCursor:
        source = self._get_standard_stage_directory_path(repo_path)
        destination = self._get_standard_stage_directory_path(destination_path)
        query = f"copy files into {destination} from {source}"
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
