from snowflake.cli.plugins.object.stage.manager import StageManager


class GitManager(StageManager):
    def show_branches(self, repo_name: str):
        query = f"show git branches in {repo_name}"
        return self._execute_query(query)

    def show_tags(self, repo_name: str):
        query = f"show git tags in {repo_name}"
        return self._execute_query(query)

    def show_files(self, repo_path: str):
        query = f"ls {repo_path}"
        return self._execute_query(query)

    def fetch(self, repo_name: str):
        query = f"alter git repository {repo_name} fetch"
        return self._execute_query(query)

    def copy(self, repo_path: str, destination_path: str):
        pass
