import functools
from typing import Optional

from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor
from snowflake.cli.api.secure_path import SecurePath
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

    @property
    def model(self):
        return self._entity_model  # noqa

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
                SecurePath(file).copy(output_file)
            elif file.is_dir():
                output_file.mkdir(parents=True, exist_ok=True)
                SecurePath(file).copy(output_file, dirs_exist_ok=True)

                output_files.append(output_file)

        return output_files

    def action_deploy(self, action_ctx: ActionContext, *args, **kwargs):
        # After adding bundle map- we should use it's mapping here

        query = self.get_deploy_sql(action_ctx, *args, **kwargs)
        result = self._sql_executor.execute_query(query)
        return result

    def action_drop(self, action_ctx: ActionContext, *args, **kwargs):
        return self._sql_executor.execute_query(self.get_drop_sql(action_ctx))

    def action_execute(
        self, action_ctx: ActionContext, *args, **kwargs
    ) -> SnowflakeCursor:
        return self._sql_executor.execute_query(self.get_execute_sql(action_ctx))

    def action_get_url(
        self, action_ctx: ActionContext, *args, **kwargs
    ):  # maybe this should be a property
        name = self._entity_model.fqn.using_connection(self._conn)
        return make_snowsight_url(
            self._conn, f"/#/streamlit-apps/{name.url_identifier}"
        )

    def get_deploy_sql(
        self,
        action_ctx: ActionContext,
        if_not_exists: bool = False,
        replace: bool = False,
        from_stage_name: Optional[str] = None,
        *args,
        **kwargs,
    ):

        if replace:
            query = "CREATE OR REPLACE "
        elif if_not_exists:
            query = "CREATE IF NOT EXISTS "
        else:
            query = "CREATE "

        query += f"STREAMLIT {self._entity_model.fqn.sql_identifier} \n"

        if from_stage_name:
            query += f" ROOT_LOCATION = '{from_stage_name}' \n"

        query += f" MAIN_FILE = '{self._entity_model.main_file}' \n"

        if self.model.imports:
            query += self.model.get_imports_sql() + "\n"

        if self.model.query_warehouse:
            query += f" QUERY_WAREHOUSE = '{self.model.query_warehouse}' \n"

        if self.model.title:
            query += f" TITLE = '{self.model.title}' \n"

        if self.model.comment:
            query += f" COMMENT = '{self.model.comment}' \n"

        if self.model.external_access_integrations:
            query += self.model.get_external_access_integrations_sql() + "\n"

        if self.model.secrets:
            query += self.model.get_secrets_sql() + "\n"

        return query

    def action_share(
        self, action_ctx: ActionContext, to_role: str, *args, **kwargs
    ) -> SnowflakeCursor:
        return self._sql_executor.execute_query(
            self.get_usage_grant_sql(action_ctx, to_role)
        )

    def get_drop_sql(self, action_ctx: ActionContext, *args, **kwargs):
        return f"DROP STREAMLIT {self._entity_model.fqn}"

    def get_execute_sql(self, action_ctx: ActionContext, *args, **kwargs):
        return f"EXECUTE STREAMLIT {self._entity_model.fqn}()"

    def get_usage_grant_sql(
        self, action_ctx: ActionContext, to_role: str, *args, **kwargs
    ) -> str:
        return f"GRANT USAGE ON STREAMLIT {{self._entity_model.fqn}} to role {to_role}"
