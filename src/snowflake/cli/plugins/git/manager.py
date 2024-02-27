from snowflake.cli.api.sql_execution import SqlExecutionMixin


class GitManager(SqlExecutionMixin):
    def show_branches(self, repo_name: str):
        query = f"show git branches in {repo_name}"
        return self._execute_query(query)

    def show_tags(self, repo_name: str):
        query = f"show git tags in {repo_name}"
        return self._execute_query(query)

    def show_files(self, repo_path: str):
        query = f"ls {repo_path}"
        return self._execute_query(query)
