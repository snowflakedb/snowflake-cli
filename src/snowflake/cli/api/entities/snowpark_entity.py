from typing import Generic, TypeVar

from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.project.schemas.entities.snowpark_entity import (
    FunctionEntityModel,
    ProcedureEntityModel,
)

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
