import functools
from textwrap import dedent

from snowflake.cli._plugins.notebook.notebook_entity_model import NotebookEntityModel
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector.cursor import SnowflakeCursor


class NotebookEntity(EntityBase[NotebookEntityModel]):
    """
    A notebook.
    """

    @functools.cached_property
    def _sql_executor(self):
        return get_sql_executor()

    @functools.cached_property
    def _fqn(self) -> FQN:
        conn = get_cli_context().connection
        return self.model.fqn.using_connection(conn)

    @functools.cached_property
    def _notebook_file_stage_path(self) -> StagePath:
        stage_name = self.model.stage_name
        filename = self.model.notebook_file
        return StagePath.from_stage_str(f"@stage_name/{self._fqn.name}/{filename}")

    def _upload_notebook_file_to_stage(self):
        cli_console.step("x")

    def _create_from_stage(self, replace: bool) -> SnowflakeCursor:
        stage_path = self._notebook_file_stage_path()
        cli_console.step("Creating notebook")
        create_str = "CREATE OR REPLACE" if replace else "CREATE"
        queries = dedent(
            f"""
            {create_str} NOTEBOOK {self._fqn.sql_identifier}
            FROM '{stage_path.parent}'
            QUERY_WAREHOUSE = '{self.model.query_warehouse}'
            MAIN_FILE = '{stage_path.name}';
            // Cannot use IDENTIFIER(...)
            ALTER NOTEBOOK {self._fqn.identifier} ADD LIVE VERSION FROM LAST;
            """
        )
        return self._sql_executor.execute_query(queries)

    def _object_exists(self) -> bool:
        return False

    def action_deploy(
        self,
        action_ctx: ActionContext,
        replace: bool,
        if_not_exists: bool,
        *args,
        **kwargs,
    ):
        with cli_console.phase(f"Deploying notebook {self._fqn}"):
            if self._object_exists():
                if if_not_exists:
                    cli_console.step("Notebook already exists, skipping...")
                    return
                if not replace:
                    # raise
                    pass

            self._upload_notebook_file_to_stage()
            self._create_from_file(replace=replace)
