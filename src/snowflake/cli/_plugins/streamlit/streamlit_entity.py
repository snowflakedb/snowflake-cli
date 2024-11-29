import functools
import shutil

from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor
from snowflake.connector.cursor import SnowflakeCursor


class StreamlitEntity(EntityBase[StreamlitEntityModel]):
    """
    A Streamlit app.
    """

    @property
    def root(self):
        return self._workspace_ctx.project_root

    @property
    def artifacts(self):
        return self._entity_model.artifacts

    @functools.cached_property
    def _sql_executor(self):
        return get_sql_executor()

    @functools.cached_property
    def _conn(self):
        return self._sql_executor._conn  # noqa

    def action_bundle(self, ctx: ActionContext, *args, **kwargs):
        # get all files from the model
        artifacts = self._entity_model.artifacts
        # get root
        output_folder = self.root / "output" / self._entity_model.stage
        output_folder.mkdir(parents=True, exist_ok=True)

        output_files = []

        # This is far from , but will be replaced by bundlemap mappings.
        for file in artifacts:
            output_file = output_folder / file.name

            if file.is_file():
                shutil.copy(file, output_file)
            elif file.is_dir():
                output_file.mkdir(parents=True, exist_ok=True)
                shutil.copytree(file, output_file, dirs_exist_ok=True)

                output_files.append(output_file)

        return output_files

    def action_deploy(self, action_ctx: ActionContext, *args, **kwargs):
        # After adding bundle map- we should use it's mapping here

        query = self.action_get_deploy_sql(action_ctx, *args, **kwargs)
        result = self._sql_executor.execute_query(query)
        return result

    def action_drop(self, action_ctx: ActionContext, *args, **kwargs):
        return self._sql_executor.execute_query(self.action_get_drop_sql(action_ctx))

    def action_execute(
        self, action_ctx: ActionContext, *args, **kwargs
    ) -> SnowflakeCursor:
        return self._sql_executor.execute_query(self.action_get_execute_sql(action_ctx))

    def action_get_url(
        self, action_ctx: ActionContext, *args, **kwargs
    ):  # maybe this should be a property
        name = self._entity_model.fqn.using_connection(self._conn)
        return make_snowsight_url(
            self._conn, f"/#/streamlit-apps/{name.url_identifier}"
        )

    def action_get_deploy_sql(self, action_ctx: ActionContext, *args, **kwargs):
        pass

    def action_share(
        self, action_ctx: ActionContext, to_role: str, *args, **kwargs
    ) -> SnowflakeCursor:
        return self._sql_executor.execute_query(self.get_share_sql(action_ctx, to_role))

    def action_get_drop_sql(self, action_ctx: ActionContext, *args, **kwargs):
        return f"DROP STREAMLIT {self._entity_model.fqn}"

    def action_get_execute_sql(self, action_ctx: ActionContext, *args, **kwargs):
        return f"EXECUTE STREAMLIT {self._entity_model.fqn}()"

    def get_share_sql(
        self, action_ctx: ActionContext, to_role: str, *args, **kwargs
    ) -> str:
        return f"GRANT USAGE ON STREAMLIT {{self._entity_model.fqn}} to role {to_role}"
