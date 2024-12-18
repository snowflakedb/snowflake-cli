import os
from pathlib import Path
from typing import Generic, Optional, TypeVar

from snowflake.cli._plugins.nativeapp.artifacts import build_bundle
from snowflake.cli._plugins.nativeapp.entities.application_package_child_interface import (
    ApplicationPackageChildInterface,
)
from snowflake.cli._plugins.nativeapp.feature_flags import FeatureFlag
from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    FunctionEntityModel,
    ProcedureEntityModel,
)
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.project.schemas.v1.native_app.path_mapping import PathMapping

T = TypeVar("T")


# WARNING: The Function/Procedure entities are not implemented yet. The logic below is only for demonstrating the
# required interfaces for composability (used by ApplicationPackageEntity behind a feature flag).
class SnowparkEntity(EntityBase[Generic[T]], ApplicationPackageChildInterface):
    def __init__(self, *args, **kwargs):
        if not FeatureFlag.ENABLE_NATIVE_APP_CHILDREN.is_enabled():
            raise NotImplementedError("Snowpark entities are not implemented yet")
        super().__init__(*args, **kwargs)

    @property
    def project_root(self) -> Path:
        return self._workspace_ctx.project_root

    @property
    def deploy_root(self) -> Path:
        return self.project_root / "output" / "deploy"

    def action_bundle(
        self,
        *args,
        **kwargs,
    ):
        return self.bundle()

    def bundle(self, bundle_root=None):
        return build_bundle(
            self.project_root,
            bundle_root or self.deploy_root,
            [
                PathMapping(src=str(artifact.src), dest=artifact.dest)
                for artifact in self._entity_model.artifacts
            ],
        )

    def _get_identifier_for_sql(
        self, arg_names: bool = True, schema: Optional[str] = None
    ) -> str:
        model = self._entity_model
        if arg_names:
            signature = ", ".join(
                f"{arg.name} {arg.arg_type}" for arg in model.signature
            )
        else:
            signature = ", ".join(arg.arg_type for arg in model.signature)
        entity_id = self.entity_id
        object_name = f"{schema}.{entity_id}" if schema else entity_id
        return f"{object_name}({signature})"

    def get_deploy_sql(
        self,
        artifacts_dir: Optional[Path] = None,
        schema: Optional[str] = None,
    ):
        model = self._entity_model
        imports = [f"'{x}'" for x in model.imports]
        if artifacts_dir:
            for root, _, files in os.walk(self.deploy_root / artifacts_dir):
                for f in files:
                    file_path_relative_to_deploy_root = (
                        Path(root).relative_to(self.deploy_root) / f
                    )
                    imports.append(f"'{str(file_path_relative_to_deploy_root)}'")

        entity_type = model.get_type().upper()

        query = [
            f"CREATE OR REPLACE {entity_type} {self._get_identifier_for_sql(schema=schema)}",
            f"RETURNS {model.returns}",
            "LANGUAGE python",
            "RUNTIME_VERSION=3.8",
            f"IMPORTS=({', '.join(imports)})",
            f"HANDLER='{model.handler}'",
            "PACKAGES=('snowflake-snowpark-python');",
        ]
        return "\n".join(query)

    def get_usage_grant_sql(self, app_role: str, schema: Optional[str] = None):
        entity_type = self._entity_model.get_type().upper()
        return f"GRANT USAGE ON {entity_type} {self._get_identifier_for_sql(schema=schema, arg_names=False)} TO APPLICATION ROLE {app_role};"


class FunctionEntity(SnowparkEntity[FunctionEntityModel]):
    """
    A single UDF
    """

    pass


class ProcedureEntity(SnowparkEntity[ProcedureEntityModel]):
    """
    A stored procedure
    """

    pass
