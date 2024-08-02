from snowflake.cli.api.entities.common import EntityBase


class ApplicationPackageEntity(EntityBase):
    """
    A Native App application package.
    """

    def bundle(self):
        raise NotImplementedError("Application package bundle")
