from typing import Literal

from snowflake.cli.api.project.schemas.entities.common import EntityModelBase
from snowflake.cli.api.project.schemas.updatable_model import DiscriminatorField


class ImageRepositoryEntityModel(EntityModelBase):
    type: Literal["image-repository"] = DiscriminatorField()  # noqa: A003
