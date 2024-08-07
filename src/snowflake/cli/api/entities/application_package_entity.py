from pathlib import Path

from snowflake.cli._plugins.nativeapp.artifacts import build_bundle

# from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli._plugins.nativeapp.codegen.compiler import NativeAppCompiler
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.project.schemas.entities.application_package_entity_model import (
    ApplicationPackageEntityModel,
)


class ApplicationPackageEntity(EntityBase):
    """
    A Native App application package.
    """

    _entity_model: ApplicationPackageEntityModel

    # def bundle(self, ws: WorkspaceManager):
    def bundle(self, ws):
        model = self._entity_model
        bundle_map = build_bundle(
            ws.project_root(), Path(model.deploy_root), model.artifacts
        )
        bundle_context = BundleContext(
            package_name=model.name,
            artifacts=model.artifacts,
            project_root=ws.project_root(),
            bundle_root=Path(model.bundle_root),
            deploy_root=Path(model.deploy_root),
            generated_root=Path(model.generated_root),
        )
        compiler = NativeAppCompiler(bundle_context)
        compiler.compile_artifacts()
        return bundle_map
