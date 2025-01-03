from typing import TypeVar

from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.entities.common import EntityActions, EntityBase

T = TypeVar("T")


class BundleAndDeploy(EntityBase[T]):
    """
    Base class for entities that can be bundled and deployed
    Provides basic action logic and abstract methods for bundle and deploy- to be implemented
    using subclass specific logic
    """

    def action_bundle(self, action_ctx: ActionContext, *args, **kwargs):
        if dependent_entities := self.dependent_entities(action_ctx):
            for dependency in dependent_entities:
                entity = action_ctx.get_entity(dependency.entity_id)
                # TODO think how to pass arguments for dependencies
                if entity.supports(EntityActions.BUNDLE):
                    entity.bundle()

        return self.bundle(*args, **kwargs)

    def action_deploy(self, action_ctx: ActionContext, *args, **kwargs):
        if dependent_entities := self.dependent_entities(action_ctx):
            for dependency in dependent_entities:
                entity = action_ctx.get_entity(dependency.entity_id)
                if entity.supports(EntityActions.DEPLOY):
                    entity.deploy()
        return self.deploy(action_ctx, *args, **kwargs)

    def bundle(self, *args, **kwargs):
        raise NotImplementedError("Bundle method should be implemented in subclass")

    def deploy(self, *args, **kwargs):
        raise NotImplementedError("Deploy method should be implemented in subclass")
