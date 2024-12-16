import functools
from typing import Generic, TypeVar

from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    FunctionEntityModel,
    ProcedureEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor

T = TypeVar("T")


class SnowparkEntity(EntityBase[Generic[T]]):

    @property
    def project_root(self):
        return self._workspace_ctx.project_root

    @functools.cached_property
    def _sql_executor(self): # maybe this could be moved to parent class, as it is used in streamlit entity as well
        return get_sql_executor()

    @functools.cached_property
    def _conn(self):
        return self._sql_executor._conn  # noqa

    @property
    def model(self):
        return self._entity_model  # noqa

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

    def bundle(self, bundle_root):
        pass

    def get_deploy_sql(self):
        pass

    def get_describe_sql(self):
        pass

    def get_drop_sql(self):
        pass

    def get_usage_grant_sql(self):

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
