from pathlib import Path

from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.connection.util import make_snowsight_url

from .exceptions import NotebookStagePathError
from .types import NotebookName, NotebookStagePath


class NotebookManager(SqlExecutionMixin):
    def execute(self, notebook_name: NotebookName):
        query = f"EXECUTE NOTEBOOK {notebook_name}()"
        return self._execute_query(query=query)

    def get_url(self, notebook_name: NotebookName):
        fqn = FQN.from_string(notebook_name).using_connection(self._conn)
        return make_snowsight_url(
            self._conn,
            f"/#/notebooks/{fqn.url_identifier}",
        )

    @staticmethod
    def _parse_stage_path(notebook_file: NotebookName) -> Path:
        if not notebook_file.endswith(".ipynb"):
            raise NotebookStagePathError(notebook_file)
        stage_path = Path(notebook_file)
        if len(stage_path.parts) < 2:
            raise NotebookStagePathError(notebook_file)

        return stage_path

    def create(self, notebook_name: NotebookName, notebook_file: NotebookStagePath):
        stage_path = self._parse_stage_path(notebook_file)

        create_query = (
            f"CREATE OR REPLACE NOTEBOOK {notebook_name.upper()}"
            f" FROM '{stage_path.parent}'"
            f" QUERY_WAREHOUSE = '{cli_context.connection.warehouse}'"
            f" MAIN_FILE = '{stage_path.name}';"
        )
        alter_version_query = (
            f"ALTER NOTEBOOK {notebook_name.upper()} ADD LIVE VERSION FROM LAST;"
        )

        queries = "\n".join((create_query, alter_version_query))
        return self._execute_queries(queries=queries)
