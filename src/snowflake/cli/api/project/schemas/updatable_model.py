from typing import Any, Dict, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.util import IDENTIFIER_NO_LENGTH
from typing_extensions import get_origin


class UpdatableModel(BaseModel):
    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    def __init__(self, *args, **kwargs):
        try:
            super().__init__(**kwargs)
        except ValidationError as e:
            raise SchemaValidationError(e)

    def update_from_dict(self, update_values: Dict[str, Any]) -> object:
        for field, value in update_values.items():
            if field in self.model_fields.keys():
                if (
                    self._is_field_an_updatable_model(field)
                    and field in self.model_fields_set
                ):
                    getattr(self, field).update_from_dict(value)
                else:
                    setattr(self, field, value)
        return self

    def _is_field_an_updatable_model(self, name: str) -> bool:
        typing_types = (Literal, list)
        field_type = self.model_fields.get(name, None)
        if field_type:
            if (args := get_args(field_type.annotation)) and not any(
                [get_origin(arg) in typing_types for arg in args]
            ):
                return any([issubclass(arg, UpdatableModel) for arg in args])
            return issubclass(type(field_type.annotation), UpdatableModel)
        return False


def IdentifierField(*args, **kwargs):  # noqa
    return Field(max_length=254, pattern=IDENTIFIER_NO_LENGTH, *args, **kwargs)
