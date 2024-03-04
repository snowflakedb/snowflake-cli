from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field
from snowflake.cli.api.project.util import IDENTIFIER_NO_LENGTH


class UpdatableModel(BaseModel):
    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    def update_from_dict(
        self, update_values: Dict[str, Any]
    ):  # this method works wrong for optional fields set to None
        for field, value in update_values.items():  # do we even need this?
            if getattr(self, field, None):
                setattr(self, field, value)
        return self


def IdentifierField(*args, **kwargs):  # noqa
    return Field(max_length=254, pattern=IDENTIFIER_NO_LENGTH, *args, **kwargs)
