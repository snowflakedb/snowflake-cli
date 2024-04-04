from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.connection.util import make_snowsight_url


class NotebookManager(SqlExecutionMixin):
    def execute(self, notebook_name: str):
        query = f"EXECUTE NOTEBOOK {notebook_name}()"
        return self._execute_query(query=query)

    def get_url(self, notebook_name: str):
        fully_qualified_name = self.to_fully_qualified_name(notebook_name)
        return make_snowsight_url(
            self._conn,
            f"/#/notebooks/{self.qualified_name_for_url(fully_qualified_name)}",
        )
