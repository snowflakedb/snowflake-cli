from snowflake.cli.api.entities.common import EntityBase


class ApplicationPackageEntity(EntityBase):
    def bundle(self):
        raise NotImplementedError("Application package bundle")
