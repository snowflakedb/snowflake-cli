import functools
from textwrap import dedent
from typing import Dict

from click import ClickException
from snowflake.cli._plugins.notebook.notebook_entity_model import NotebookEntityModel
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector import ProgrammingError, SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

_DEFAULT_NOTEBOOK_STAGE_NAME = "@notebooks"


class NotebookEntity(EntityBase[NotebookEntityModel]):
    """
    A notebook.
    """

    @functools.cached_property
    def _sql_executor(self):
        return get_sql_executor()

    @functools.cached_property
    def _connection(self) -> SnowflakeConnection:
        return get_cli_context().connection

    @functools.cached_property
    def _fqn(self) -> FQN:
        return self.model.fqn.using_connection(self._connection)

    @functools.cached_property
    def _notebook_file_stage_path(self) -> StagePath:
        filename = self.model.notebook_file.name
        stage_path = self.model.stage_path
        if stage_path is None:
            stage_path = f"{_DEFAULT_NOTEBOOK_STAGE_NAME}/{self._fqn.name}"
        return StagePath.from_stage_str(f"{stage_path}/{filename}")

    def _object_exists(self) -> bool:
        # currently notebook objects are not supported by object manager - re-implementing "exists"
        try:
            self._sql_executor.execute_query(
                f"DESCRIBE NOTEBOOK {self._fqn.sql_identifier}"
            )
            return True
        except ProgrammingError:
            return False

    def _upload_notebook_file_to_stage(self, overwrite):
        stage_path = self._notebook_file_stage_path
        stage_fqn = FQN.from_stage(stage_path.stage).using_connection(self._connection)
        stage_manager = StageManager()

        cli_console.step(f"Creating stage {stage_fqn} if not exists")
        stage_manager.create(fqn=stage_fqn)
        cli_console.step(f"Uploading {self.model.notebook_file} to {stage_path.parent}")
        stage_manager.put(
            local_path=self.model.notebook_file,
            stage_path=str(stage_path.parent),
            overwrite=overwrite,
        )

    def _create_from_stage(self, replace: bool) -> SnowflakeCursor:
        stage_path = self._notebook_file_stage_path
        cli_console.step("Creating notebook")
        create_str = "CREATE OR REPLACE" if replace else "CREATE"
        queries = dedent(
            f"""
            {create_str} NOTEBOOK {self._fqn.sql_identifier}
            FROM '{stage_path.stage_with_at}'
            QUERY_WAREHOUSE = '{self.model.query_warehouse}'
            MAIN_FILE = '{stage_path.path}';
            // Cannot use IDENTIFIER(...)
            ALTER NOTEBOOK {self._fqn.identifier} ADD LIVE VERSION FROM LAST;
            """
        )
        return self._sql_executor.execute_query(queries)

    def action_deploy(
        self,
        action_ctx: ActionContext,
        replace: bool,
        if_not_exists: bool,
        *args,
        **kwargs,
    ) -> Dict[str, str]:
        def _result(status: str) -> Dict[str, str]:
            return {"object": self._fqn.name, "status": status}

        success_status = "CREATED"
        with cli_console.phase(f"Deploying notebook {self._fqn}"):
            if self._object_exists():
                if if_not_exists:
                    cli_console.step("Notebook already exists, skipping.")
                    return _result("SKIPPED")
                if not replace:
                    raise ClickException(
                        f"Notebook {self._fqn.name} already exists. Consider using --replace."
                    )
                success_status = "REPLACED"

            self._upload_notebook_file_to_stage(overwrite=replace)
            self._create_from_stage(replace=replace)
            return _result(success_status)
