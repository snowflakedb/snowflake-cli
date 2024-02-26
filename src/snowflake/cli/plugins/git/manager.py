from snowflake.cli.api.sql_execution import SqlExecutionMixin


class GitManager(SqlExecutionMixin):
    def show(self, object_type: str, repo_name: str):
        assert object_type in ["branches", "tags"]
        query = f"show git {object_type} in {repo_name}"
        return self._execute_query(query)
