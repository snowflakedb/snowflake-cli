from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.project.schemas.entities.application_entity_model import (
    ApplicationEntityModel,
)


class ApplicationEntity(EntityBase[ApplicationEntityModel]):
    """
    A Native App application object, created from an application package.
    """

    pass
