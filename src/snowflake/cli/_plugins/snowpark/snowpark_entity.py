from typing import Generic, TypeVar

from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    FunctionEntityModel,
    ProcedureEntityModel,
)
from snowflake.cli.api.entities.common import EntityBase

T = TypeVar("T")


class SnowparkEntity(EntityBase[Generic[T]]):
    pass


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
