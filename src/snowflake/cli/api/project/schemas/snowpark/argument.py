from typing import Optional

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class Argument(UpdatableModel):
    name: str = Field(title="Name of the argument")
    arg_type: str = Field(
        title="Type of the argument", alias="type"
    )  # TODO: consider introducing literal/enum here
    default: Optional[str] = Field(title="Default value for an argument", default=None)
