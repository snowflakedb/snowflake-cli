from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.util import IDENTIFIER_NO_LENGTH


class UpdatableModel(BaseModel):
    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    def __init__(self, *args, **kwargs):
        try:
            super().__init__(**kwargs)
        except ValidationError as e:
            raise SchemaValidationError(e)

    def update_from_dict(
        self, update_values: Dict[str, Any]
    ):  # this method works wrong for optional fields set to None
        for field, value in update_values.items():  # do we even need this?
            if getattr(self, field, None):
                setattr(self, field, value)
        return self


def IdentifierField(*args, **kwargs):  # noqa
    return Field(max_length=254, pattern=IDENTIFIER_NO_LENGTH, *args, **kwargs)
