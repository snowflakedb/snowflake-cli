from pathlib import Path
from typing import Optional

from snowflake.cli._plugins.nativeapp.artifacts import build_bundle
from snowflake.cli._plugins.nativeapp.entities.application_package_child_interface import (
    ApplicationPackageChildInterface,
)
from snowflake.cli._plugins.nativeapp.feature_flags import FeatureFlag
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.project.schemas.v1.native_app.path_mapping import PathMapping


# WARNING: This entity is not implemented yet. The logic below is only for demonstrating the
# required interfaces for composability (used by ApplicationPackageEntity behind a feature flag).
class StreamlitEntity(
    EntityBase[StreamlitEntityModel], ApplicationPackageChildInterface
):
    """
    A Streamlit app.
    """

    @property
    def project_root(self) -> Path:
        return self._workspace_ctx.project_root

    @property
    def deploy_root(self) -> Path:
        return self.project_root / "output" / "deploy"

    def _verify_feature_flag_enabled(self):
        if not FeatureFlag.ENABLE_NATIVE_APP_CHILDREN.is_enabled():
            raise NotImplementedError("Streamlit entity is not implemented yet")

    def action_bundle(
        self,
        action_ctx: ActionContext,
        *args,
        **kwargs,
    ):
        self._verify_feature_flag_enabled()
        return self.bundle()

    def bundle(self, bundle_root=None):
        self._verify_feature_flag_enabled()
        return build_bundle(
            self.project_root,
            bundle_root or self.deploy_root,
            [
                PathMapping(src=str(artifact))
                for artifact in self._entity_model.artifacts
            ],
        )

    def get_deploy_sql(
        self,
        artifacts_dir: Optional[Path] = None,
        schema: Optional[str] = None,
    ):
        self._verify_feature_flag_enabled()
        entity_id = self.entity_id
        if artifacts_dir:
            streamlit_name = f"{schema}.{entity_id}" if schema else entity_id
            return f"CREATE OR REPLACE STREAMLIT {streamlit_name} FROM '{artifacts_dir}' MAIN_FILE='{self._entity_model.main_file}';"
        else:
            return f"CREATE OR REPLACE STREAMLIT {entity_id} MAIN_FILE='{self._entity_model.main_file}';"

    def get_usage_grant_sql(self, app_role: str, schema: Optional[str] = None):
        self._verify_feature_flag_enabled()
        entity_id = self.entity_id
        streamlit_name = f"{schema}.{entity_id}" if schema else entity_id
        return (
            f"GRANT USAGE ON STREAMLIT {streamlit_name} TO APPLICATION ROLE {app_role};"
        )
