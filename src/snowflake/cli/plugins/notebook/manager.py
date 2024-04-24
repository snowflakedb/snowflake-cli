from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.connection.util import make_snowsight_url


class NotebookManager(SqlExecutionMixin):
    def execute(self, notebook_name: str):
        query = f"EXECUTE NOTEBOOK {notebook_name}()"
        return self._execute_query(query=query)

    def get_url(self, notebook_name: str):
        return make_snowsight_url(
            self._conn,
            f"/#/notebooks/{self.qualified_name_for_url(notebook_name)}",
        )
