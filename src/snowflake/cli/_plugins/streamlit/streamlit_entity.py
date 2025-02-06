from pathlib import Path
from typing import Optional

from click import ClickException
from snowflake.connector import ProgrammingError
from snowflake.core.stage import StageResource, Stage

from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.nativeapp.artifacts import build_bundle
from snowflake.cli._plugins.nativeapp.feature_flags import FeatureFlag
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.project.project_paths import bundle_root
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.connector.cursor import SnowflakeCursor


class StreamlitEntity(EntityBase[StreamlitEntityModel]):
    """
    A Streamlit app.
    """

    def __init__(self, *args, **kwargs):
        if not FeatureFlag.ENABLE_NATIVE_APP_CHILDREN.is_enabled():
            raise NotImplementedError("Streamlit entity is not implemented yet")
        super().__init__(*args, **kwargs)

    @property
    def root(self):
        return self._workspace_ctx.project_root

    @property
    def artifacts(self):
        return self._entity_model.artifacts

    def action_bundle(self, action_ctx: ActionContext, *args, **kwargs):
        return self.bundle()

    def action_deploy(self, action_ctx: ActionContext, *args, **kwargs):
        # After adding bundle map- we should use it's mapping here
        # To copy artifacts to destination on stage.

        return self.deploy()

    def action_describe(self, action_ctx: ActionContext, *args, **kwargs):
        return self.describe()

    def action_drop(self, action_ctx: ActionContext, *args, **kwargs):
        return self._execute_query(self.get_drop_sql())

    def action_execute(
        self, action_ctx: ActionContext, *args, **kwargs
    ) -> SnowflakeCursor:
        return self._execute_query(self.get_execute_sql())

    def action_get_url(
        self, action_ctx: ActionContext, *args, **kwargs
    ):  # maybe this should be a property
        name = self._entity_model.fqn.using_connection(self._conn)
        return make_snowsight_url(
            self._conn, f"/#/streamlit-apps/{name.url_identifier}"
        )

    def bundle(self, output_dir: Optional[Path] = None) -> BundleMap:
        return build_bundle(
            self.root,
            output_dir or bundle_root(self.root, "streamlit") / self.entity_id,
            [
                PathMapping(
                    src=artifact.src, dest=artifact.dest, processors=artifact.processors
                )
                for artifact in self._entity_model.artifacts
            ],
        )

    def deploy(self, _open: bool, replace: bool, bundle_map: Optional[BundleMap] = None, experimental: Optional[bool] = False, *args, **kwargs):
        if bundle_map is None: #TODO: maybe we could hold bundle map as a cached property?
            bundle_map = self.bundle()

        console = self._workspace_ctx.console
        console.step(f"Checking if object exists")
        if self._object_exists() and not replace:
            raise ClickException(
                f"Streamlit {self.model.fqn.sql_identifier} already exists. Use 'replace' option to overwrite."
                )

        console.step(f"Creating stage {self.model.stage} if not exists")
        stage = self._create_stage_if_not_exists()
        use_versioned_stage = FeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled()

        if experimental or use_versioned_stage or FeatureFlag.ENABLE_STREAMLIT_EMBEDDED_STAGE.is_enabled():
            if replace:
                raise ClickException(f"Cannot specify both replace and experimental for deploying entity {self.entity_id}")

            self._execute_query(self.get_deploy_sql(
                if_not_exists=True,
                replace=replace,

            ))

        console.step(f"Uploading artifacts to stage {self.model.stage}")
        self._upload_files_to_stage(stage, bundle_map)

        console.step(f"Creating Streamlit object {self.model.fqn.sql_identifier}")

        return self._execute_query(self.get_deploy_sql(replace=replace,from_stage_name=self.model.stage))

    def describe(self) -> SnowflakeCursor:
        return self._execute_query(self.get_describe_sql())

    def action_share(
        self, action_ctx: ActionContext, to_role: str, *args, **kwargs
    ) -> SnowflakeCursor:
        return self._execute_query(self.get_share_sql(to_role))

    def get_deploy_sql(
        self,
        if_not_exists: bool = False,
        replace: bool = False,
        from_stage_name: Optional[str] = None,
        artifacts_dir: Optional[Path] = None,
        schema: Optional[str] = None,
        *args,
        **kwargs,
    ) -> str:
        if replace and if_not_exists:
            raise ClickException("Cannot specify both replace and if_not_exists")

        if replace:
            query = "CREATE OR REPLACE "
        elif if_not_exists:
            query = "CREATE IF NOT EXISTS "
        else:
            query = "CREATE "

        schema_to_use = schema or self._entity_model.fqn.schema
        query += f"STREAMLIT {self._entity_model.fqn.set_schema(schema_to_use).sql_identifier}"

        if from_stage_name:
            query += f"\nROOT_LOCATION = '{from_stage_name}'"
        elif artifacts_dir:
            query += f"\nFROM '{artifacts_dir}'"

        query += f"\nMAIN_FILE = '{self._entity_model.main_file}'"

        if self.model.imports:
            query += "\n" + self.model.get_imports_sql()

        if self.model.query_warehouse:
            query += f"\nQUERY_WAREHOUSE = '{self.model.query_warehouse}'"

        if self.model.title:
            query += f"\nTITLE = '{self.model.title}'"

        if self.model.comment:
            query += f"\nCOMMENT = '{self.model.comment}'"

        if self.model.external_access_integrations:
            query += "\n" + self.model.get_external_access_integrations_sql()

        if self.model.secrets:
            query += "\n" + self.model.get_secrets_sql()

        return query + ";"

    def get_describe_sql(self) -> str:
        return f"DESCRIBE STREAMLIT {self.model.fqn.sql_identifier};"


    def get_share_sql(self, to_role: str) -> str:
        return f"GRANT USAGE ON STREAMLIT {self.model.fqn.sql_identifier} TO ROLE {to_role};"

    def get_execute_sql(self):
        return f"EXECUTE STREAMLIT {self._entity_model.fqn}();"

    def get_usage_grant_sql(self, app_role: str, schema: Optional[str] = None) -> str:
        entity_id = self.entity_id
        streamlit_name = f"{schema}.{entity_id}" if schema else entity_id
        return (
            f"GRANT USAGE ON STREAMLIT {streamlit_name} TO APPLICATION ROLE {app_role};"
        )

    def _create_stage_if_not_exists(self)-> StageResource: #Another candidate to be moved to parent class
        stage_collection = self.snow_api_root.databases[
            self.fqn.database or self._conn.database
        ].schemas[self.fqn.schema or self._conn.schema].stages

        if not stage_collection[self.model.stage]:
            stage_object = Stage(name=self.model.stage)
            stage_collection.create(stage_object)

        return stage_collection[self.model.stage]

    def _object_exists(self):
        try:
            self.describe()
            return True
        except ProgrammingError:
            return False

    @staticmethod #TODO: maybe a good candidate to transfer to parent class
    def _upload_files_to_stage(stage: StageResource, bundle_map: BundleMap):
        for src, dest in bundle_map.all_mappings(absolute=True, expand_directories=True):
            stage.put(local_path=src, stage_path=dest, overwrite=True)
