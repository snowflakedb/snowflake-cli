from pathlib import Path

from snowflake.cli._plugins.nativeapp.artifacts import build_bundle
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli._plugins.nativeapp.codegen.compiler import NativeAppCompiler
from snowflake.cli._plugins.workspace.action_context import ActionContext
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.project.schemas.entities.application_package_entity_model import (
    ApplicationPackageEntityModel,
)


class ApplicationPackageEntity(EntityBase[ApplicationPackageEntityModel]):
    """
    A Native App application package.
    """

    def bundle(self, ctx: ActionContext):
        model = self._entity_model
        bundle_map = build_bundle(
            ctx.project_root, Path(model.deploy_root), model.artifacts
        )
        bundle_context = BundleContext(
            package_name=model.identifier,
            artifacts=model.artifacts,
            project_root=ctx.project_root,
            bundle_root=Path(model.bundle_root),
            deploy_root=Path(model.deploy_root),
            generated_root=Path(model.generated_root),
        )
        compiler = NativeAppCompiler(bundle_context)
        compiler.compile_artifacts()
        return bundle_map
