from snowflake.cli.api.entities.common import EntityBase


class SnowparkEntity(EntityBase):
    pass


class FunctionEntity(SnowparkEntity):
    """
    A single UDF
    """

    pass


class ProcedureEntity(SnowparkEntity):
    """
    A stored procedure
    """

    pass
