from enum import Enum, unique
from typing import Literal

from snowflake.cli.api.project.schemas.entities.common import EntityModelBase
from snowflake.cli.api.project.schemas.updatable_model import DiscriminatorField
from snowflake.core.stage import Stage


@unique
class KindType(Enum):
    PERMANENT = "PERMANENT"
    TEMPORARY = "TEMPORARY"


class StageEntityModel(EntityModelBase, Stage):
    type: Literal["stage"] = DiscriminatorField()  # noqa: A003
    # TODO: discuss: inherit or compose? composition would require either a lot of magic or double keying of fields,
    #       while inheritance does that for free + offers autocompletion etc
    # api_resource: Stage

    # @property
    # def kind(self) -> str:
    #     return self.api_resource.kind
    #
    # @property
    # def comment(self) -> Optional[str]:
    #     return self.api_resource.comment
    #
    # @property
    # def name(self) -> str:
    #     return self.api_resource.name
