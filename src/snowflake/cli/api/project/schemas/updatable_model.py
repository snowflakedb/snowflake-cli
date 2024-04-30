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

    def update_from_dict(self, update_values: Dict[str, Any]):
        """
        Takes a dictionary with values to override.
        If the field type is subclass of a UpdatableModel, its update_from_dict() method is called with
        the value to be set.
        If not, we use simple setattr to set new value.
        Values provided are validated against original restrictions, so it's impossible to overwrite string field with
        integer value etc.
        """
        for field, value in update_values.items():
            if field in self.model_fields.keys():
                if (
                    hasattr(getattr(self, field), "update_from_dict")
                    and field in self.model_fields_set
                ):
                    getattr(self, field).update_from_dict(value)
                else:
                    setattr(self, field, value)
        return self


def IdentifierField(*args, **kwargs):  # noqa
    return Field(max_length=254, pattern=IDENTIFIER_NO_LENGTH, *args, **kwargs)
