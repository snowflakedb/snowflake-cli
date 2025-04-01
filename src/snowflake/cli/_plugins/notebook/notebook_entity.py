import functools

from click import ClickException
from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.notebook.notebook_entity_model import NotebookEntityModel
from snowflake.cli._plugins.notebook.notebook_project_paths import NotebookProjectPaths
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.artifacts.upload import sync_artifacts_with_stage
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor

_DEFAULT_NOTEBOOK_STAGE_NAME = "@notebooks"


class NotebookEntity(EntityBase[NotebookEntityModel]):
    """
    A notebook.
    """

    @property
    def _stage_path_from_model(self) -> str:
        if self.model.stage_path is None:
            return f"{_DEFAULT_NOTEBOOK_STAGE_NAME}/{self.fqn.name}"
        return self.model.stage_path

    @functools.cached_property
    def _stage_path(self) -> StagePath:
        return StagePath.from_stage_str(self._stage_path_from_model)

    @functools.cached_property
    def _project_paths(self):
        return NotebookProjectPaths(get_cli_context().project_root)

    def _object_exists(self) -> bool:
        # currently notebook objects are not supported by object manager - re-implementing "exists"
        try:
            self.action_describe()
            return True
        except ProgrammingError:
            return False

    def get_create_sql(self, replace: bool) -> str:
        main_file_stage_path = self._stage_path / (
            self.model.notebook_file.absolute().relative_to(
                self._project_paths.project_root
            )
        )
        query = "CREATE OR REPLACE " if replace else "CREATE "
        query += (
            f"NOTEBOOK {self.fqn.sql_identifier}\n"
            f"FROM '{main_file_stage_path.stage_with_at}'\n"
            f"QUERY_WAREHOUSE = '{self.model.query_warehouse}'\n"
            f"MAIN_FILE = '{main_file_stage_path.path}'"
        )
        if self.model.compute_pool:
            query += f"\nCOMPUTE_POOL = '{self.model.compute_pool}'"
        if self.model.runtime_name:
            query += f"\nRUNTIME_NAME = '{self.model.runtime_name}'"

        query += (
            ";\n// Cannot use IDENTIFIER(...)"
            f"\nALTER NOTEBOOK {self.fqn.identifier} ADD LIVE VERSION FROM LAST;"
        )
        return query

    def action_describe(self) -> SnowflakeCursor:
        return self._sql_executor.execute_query(self.get_describe_sql())

    def action_create(self, replace: bool) -> str:
        self._sql_executor.execute_query(self.get_create_sql(replace))
        return make_snowsight_url(
            self._conn,
            f"/#/notebooks/{self.fqn.using_connection(self._conn).url_identifier}",
        )

    def action_deploy(
        self,
        action_ctx: ActionContext,
        replace: bool,
        prune: bool,
        *args,
        **kwargs,
    ) -> str:
        if self._object_exists():
            if not replace:
                raise ClickException(
                    f"Notebook {self.fqn.name} already exists. Consider using --replace."
                )
        with cli_console.phase(f"Uploading artifacts to {self._stage_path}"):
            sync_artifacts_with_stage(
                project_paths=self._project_paths,
                stage_root=self._stage_path_from_model,
                prune=prune,
                artifacts=self.model.artifacts,
            )

        with cli_console.phase(f"Creating notebook {self.fqn}"):
            return self.action_create(replace=replace)

    # complementary actions, currently not used - to be implemented in future
    def action_drop(self, *args, **kwargs):
        raise ClickException("action DROP not supported by NOTEBOOK entity")

    def action_teardown(self, *args, **kwargs):
        raise ClickException("action TEARDOWN not supported by NOTEBOOK entity")
