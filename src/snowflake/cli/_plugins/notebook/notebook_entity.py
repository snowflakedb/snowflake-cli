import functools
from textwrap import dedent

from click import ClickException
from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.notebook.notebook_entity_model import NotebookEntityModel
from snowflake.cli._plugins.notebook.notebook_project_paths import NotebookProjectPaths
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.artifacts.utils import bundle_artifacts
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor

_DEFAULT_NOTEBOOK_STAGE_NAME = "@notebooks"


class NotebookEntity(EntityBase[NotebookEntityModel]):
    """
    A notebook.
    """

    @functools.cached_property
    def _stage_path(self) -> StagePath:
        stage_path = self.model.stage_path
        if stage_path is None:
            stage_path = f"{_DEFAULT_NOTEBOOK_STAGE_NAME}/{self.fqn.name}"
        return StagePath.from_stage_str(stage_path)

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

    def _upload_artifacts(self):
        stage_fqn = FQN.from_stage(self._stage_path.stage)
        stage_manager = StageManager()
        cli_console.step(f"Creating stage {stage_fqn} if not exists")
        stage_manager.create(fqn=stage_fqn)

        cli_console.step(f"Uploading artifacts to {self._stage_path}")

        # creating bundle map to handle glob patterns logic
        bundle_map = bundle_artifacts(self._project_paths, self.model.artifacts)
        for absolute_src, absolute_dest in bundle_map.all_mappings(
            absolute=True, expand_directories=True
        ):
            artifact_stage_path = self._stage_path / (
                absolute_dest.relative_to(self._project_paths.bundle_root).parent
            )
            stage_manager.put(
                local_path=absolute_src, stage_path=artifact_stage_path, overwrite=True
            )

    def get_create_sql(self, replace: bool) -> str:
        main_file_stage_path = self._stage_path / (
            self.model.notebook_file.absolute().relative_to(
                self._project_paths.project_root
            )
        )
        create_str = "CREATE OR REPLACE" if replace else "CREATE"
        return dedent(
            f"""
            {create_str} NOTEBOOK {self.fqn.sql_identifier}
            FROM '{main_file_stage_path.stage_with_at}'
            QUERY_WAREHOUSE = '{self.model.query_warehouse}'
            MAIN_FILE = '{main_file_stage_path.path}';
            // Cannot use IDENTIFIER(...)
            ALTER NOTEBOOK {self.fqn.identifier} ADD LIVE VERSION FROM LAST;
            """
        )

    def action_describe(self) -> SnowflakeCursor:
        return self._sql_executor.execute_query(self.get_describe_sql())

    def action_create(self, replace: bool) -> str:
        cli_console.step("Creating notebook")
        self._sql_executor.execute_query(self.get_create_sql(replace))
        return make_snowsight_url(
            self._conn,
            f"/#/notebooks/{self.fqn.using_connection(self._conn).url_identifier}",
        )

    def action_deploy(
        self,
        action_ctx: ActionContext,
        replace: bool,
        *args,
        **kwargs,
    ) -> str:
        with cli_console.phase(f"Deploying notebook {self.fqn}"):
            if self._object_exists():
                if not replace:
                    raise ClickException(
                        f"Notebook {self.fqn.name} already exists. Consider using --replace."
                    )

            self._upload_artifacts()
            return self.action_create(replace=replace)

    # complementary actions, currently not used - to be implemented in future
    def action_drop(self, *args, **kwargs):
        raise ClickException("action DROP not supported by NOTEBOOK entity")

    def action_teardown(self, *args, **kwargs):
        raise ClickException("action TEARDOWN not supported by NOTEBOOK entity")
