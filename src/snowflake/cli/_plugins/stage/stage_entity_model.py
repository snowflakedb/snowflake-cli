from enum import StrEnum, unique
from typing import Literal, Optional

from snowflake.cli.api.project.schemas.entities.common import EntityModelBase
from snowflake.cli.api.project.schemas.updatable_model import DiscriminatorField
from snowflake.core.stage import Stage


@unique
class KindType(StrEnum):
    PERMANENT = "PERMANENT"
    TEMPORARY = "TEMPORARY"


class StageEntityModel(EntityModelBase):
    type: Literal["stage"] = DiscriminatorField()  # noqa: A003
    _api_resource: Stage

    def __init__(self, api_resource: Stage):
        super().__init__()
        self._api_resource = api_resource

    @property
    def kind(self) -> str:
        return self._api_resource.kind

    @property
    def comment(self) -> Optional[str]:
        return self._api_resource.comment

    @property
    def name(self) -> str:
        return self._api_resource.name
