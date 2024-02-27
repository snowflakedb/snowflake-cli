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

    def copy(self, repo_path: str, destination_path: str) -> SnowflakeCursor:
        source = self._get_standard_stage_directory_path(repo_path)
        destination = self._get_standard_stage_directory_path(destination_path)
        query = f"copy files into {destination} from {source}"
        return self._execute_query(query)
