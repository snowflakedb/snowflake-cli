from typing import Generic, TypeVar

from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    FunctionEntityModel,
    ProcedureEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.entities.common import EntityBase

T = TypeVar("T")


class SnowparkEntity(EntityBase[Generic[T]]):

    @property
    def project_root(self):
        return self._workspace_ctx.project_root

    def action_bundle(self, action_ctx: ActionContext, *args, **kwargs):
        pass

    def action_deploy(self, action_ctx: ActionContext, *args, **kwargs):
        pass

    def action_drop(self, action_ctx: ActionContext, *args, **kwargs):
        pass

    def action_describe(self, action_ctx: ActionContext, *args, **kwargs):
        pass

    def action_execute(self, action_ctx: ActionContext, *args, **kwargs):
        pass

    def get_deploy_sql(self):
        pass

    def get_usage_grant_sql

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
