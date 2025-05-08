import logging
from pathlib import Path
from typing import Optional

from click import ClickException
from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.nativeapp.artifacts import build_bundle
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.entities.utils import EntityActions, sync_deploy_root_with_stage
from snowflake.cli.api.feature_flags import FeatureFlag as GlobalFeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.project_paths import bundle_root
from snowflake.cli.api.project.schemas.entities.common import Identifier, PathMapping
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)


class StreamlitEntity(EntityBase[StreamlitEntityModel]):
    """
    A Streamlit app.
    """

    def __init__(self, *args, **kwargs):
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
        return self.deploy(action_ctx, *args, **kwargs)

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

    def deploy(
        self,
        action_context: ActionContext,
        _open: bool,
        replace: bool,
        prune: bool = False,
        bundle_map: Optional[BundleMap] = None,
        experimental: Optional[bool] = False,
        *args,
        **kwargs,
    ):
        if (
            bundle_map is None
        ):  # TODO: maybe we could hold bundle map as a cached property?
            bundle_map = self.bundle()

        console = self._workspace_ctx.console
        console.step(f"Checking if object exists")
        if self._object_exists() and not replace:
            raise ClickException(
                f"Streamlit {self.model.fqn.sql_identifier} already exists. Use 'replace' option to overwrite."
            )

        console.step(f"Creating stage {self.model.stage} if not exists")
        stage = self._create_stage_if_not_exists()

        if (
            experimental
            or GlobalFeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled()
            or GlobalFeatureFlag.ENABLE_STREAMLIT_EMBEDDED_STAGE.is_enabled()
        ):
            self._deploy_experimental(bundle_map=bundle_map, replace=replace)
        else:
            console.step(f"Uploading artifacts to stage {self.model.stage}")

            # We use a static method from StageManager here, but maybe this logic could be implemented elswhere, as we implement entities?
            name = (
                self.model.identifier.name
                if isinstance(self.model.identifier, Identifier)
                else self.model.identifier
            )
            stage_root = StageManager.get_standard_stage_prefix(
                f"{FQN.from_string(self.model.stage).using_connection(self._conn)}/{name}"
            )
            if prune:
                sync_deploy_root_with_stage(
                    console=self._workspace_ctx.console,
                    deploy_root=bundle_map.deploy_root(),
                    bundle_map=bundle_map,
                    prune=prune,
                    recursive=True,
                    stage_path=StageManager().stage_path_parts_from_str(stage_root),
                    print_diff=True,
                )
            else:
                self._upload_files_to_stage(stage, bundle_map, None)

            console.step(f"Creating Streamlit object {self.model.fqn.sql_identifier}")

            self._execute_query(
                self.get_deploy_sql(replace=replace, from_stage_name=stage_root)
            )

        return self.perform(EntityActions.GET_URL, action_context, *args, **kwargs)

    def describe(self) -> SnowflakeCursor:
        return self._execute_query(self.get_describe_sql())

    def action_share(
        self, action_ctx: ActionContext, to_role: str, *args, **kwargs
    ) -> SnowflakeCursor:
        return self._execute_query(self.get_share_sql(to_role))

    def get_add_live_version_sql(
        self, schema: Optional[str] = None, database: Optional[str] = None
    ):
        return f"ALTER STREAMLIT {self._get_identifier(schema,database)} ADD LIVE VERSION FROM LAST;"

    def get_checkout_sql(
        self, schema: Optional[str] = None, database: Optional[str] = None
    ):
        return f"ALTER STREAMLIT {self._get_identifier(schema,database)} CHECKOUT;"

    def get_deploy_sql(
        self,
        if_not_exists: bool = False,
        replace: bool = False,
        from_stage_name: Optional[str] = None,
        artifacts_dir: Optional[Path] = None,
        schema: Optional[str] = None,
        database: Optional[str] = None,
        *args,
        **kwargs,
    ) -> str:

        if replace:
            query = "CREATE OR REPLACE STREAMLIT"
        elif if_not_exists:
            query = "CREATE STREAMLIT IF NOT EXISTS"
        else:
            query = "CREATE STREAMLIT"

        query += f" {self._get_identifier(schema, database)}"

        if from_stage_name:
            query += f"\nROOT_LOCATION = '{from_stage_name}'"
        elif artifacts_dir:
            query += f"\nFROM '{artifacts_dir}'"

        query += f"\nMAIN_FILE = '{self._entity_model.main_file}'"

        if self.model.imports:
            query += "\n" + self.model.get_imports_sql()

        if self.model.query_warehouse:
            query += f"\nQUERY_WAREHOUSE = {self.model.query_warehouse}"
        else:
            self._workspace_ctx.console.warning(
                "[Deprecation] In next major version we will remove default query_warehouse='streamlit'."
            )
            query += f"\nQUERY_WAREHOUSE = 'streamlit'"

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
        return f"DESCRIBE STREAMLIT {self._get_identifier()};"

    def get_share_sql(self, to_role: str) -> str:
        return f"GRANT USAGE ON STREAMLIT {self._get_identifier()} TO ROLE {to_role};"

    def get_execute_sql(self):
        return f"EXECUTE STREAMLIT {self._get_identifier()}();"

    def get_usage_grant_sql(self, app_role: str, schema: Optional[str] = None) -> str:
        entity_id = self.entity_id
        streamlit_name = f"{schema}.{entity_id}" if schema else entity_id
        return (
            f"GRANT USAGE ON STREAMLIT {streamlit_name} TO APPLICATION ROLE {app_role};"
        )

    def _object_exists(self) -> bool:
        try:
            self.describe()
            return True
        except ProgrammingError:
            return False

    def _deploy_experimental(
        self, bundle_map: BundleMap, replace: bool = False, prune: bool = False
    ):
        self._execute_query(
            self.get_deploy_sql(
                if_not_exists=True,
                replace=replace,
            )
        )
        try:
            if GlobalFeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled():
                self._execute_query(self.get_add_live_version_sql())
            elif not GlobalFeatureFlag.ENABLE_STREAMLIT_NO_CHECKOUTS.is_enabled():
                self._execute_query(self.get_checkout_sql())
        except ProgrammingError as e:
            if "Checkout already exists" in str(
                e
            ) or "There is already a live version" in str(e):
                log.info("Checkout already exists, continuing")
            else:
                raise

        embeded_stage_name = (
            f"snow://streamlit/{self.model.fqn.using_connection(self._conn).identifier}"
        )

        if GlobalFeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled():
            stage_root = f"{embeded_stage_name}/versions/live"
        else:
            stage_root = f"{embeded_stage_name}/default_checkout"

        stage_resource = self._create_stage_if_not_exists(embeded_stage_name)

        if prune:
            sync_deploy_root_with_stage(
                console=self._workspace_ctx.console,
                deploy_root=bundle_map.deploy_root(),
                bundle_map=bundle_map,
                prune=prune,
                recursive=True,
                stage_path=StageManager().stage_path_parts_from_str(stage_root),
                print_diff=True,
            )
        else:
            self._upload_files_to_stage(stage_resource, bundle_map)
