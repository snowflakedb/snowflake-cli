# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, Iterator, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)
from pydantic.fields import FieldInfo
from snowflake.cli.api.project.util import IDENTIFIER_NO_LENGTH

PROJECT_TEMPLATE_START = "<%"


def _is_templated(info: ValidationInfo, value: Any) -> bool:
    return (
        info.context
        and info.context.get("skip_validation_on_templates", False)
        and isinstance(value, str)
        and PROJECT_TEMPLATE_START in value
    )


def field_validator_allowing_templates(*validator_args, **validator_kwargs):
    """
    This validator replaces field_validator from Pydantic.
    The difference is that this validator will skip validation
    whenever a value is a String and the value is templated.

    It also checks the context to ensure skip_validation_on_templates is set to True.
    Otherwise, if skip_validation_on_templates context is not set, it behaves the same
    way as field_validator, and does not check templates anymore.
    """
    mode = validator_kwargs.get("mode", "after")

    def decorator(func):
        def wrapper_mode_wrap(cls, value, handler, info: ValidationInfo, **kwargs):
            if _is_templated(info, value):
                return value
            return func(cls, value, handler, **kwargs)

        def wrapper_default(cls, value, info: ValidationInfo, **kwargs):
            if _is_templated(info, value):
                return value
            return func(cls, value, **kwargs)

        wrapper = wrapper_mode_wrap if mode == "wrap" else wrapper_default
        return field_validator(*validator_args, **validator_kwargs)(
            classmethod(wrapper)
        )

    return decorator


_initial_context: ContextVar[Optional[Dict[str, Any]]] = ContextVar(
    "_init_context_var", default=None
)


@contextmanager
def context(value: Dict[str, Any]) -> Iterator[None]:
    """
    Thread safe context for Pydantic.
    By using `with context()`, you ensure context changes apply
    to the with block only
    """
    token = _initial_context.set(value)
    try:
        yield
    finally:
        _initial_context.reset(token)


class UpdatableModel(BaseModel):
    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    def __init__(self, /, **data: Any) -> None:
        self.__pydantic_validator__.validate_python(
            data,
            self_instance=self,
            context=_initial_context.get(),
        )

    @classmethod
    def _is_entity_type_field(cls, field: Any) -> bool:
        if not isinstance(field, FieldInfo) or not field.json_schema_extra:
            return False

        return (
            "is_type_field" in field.json_schema_extra
            and field.json_schema_extra["is_type_field"]
        )

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Collect all the Pydantic fields
        # from all the classes in the inheritance chain
        field_annotations = {}
        field_values = {}
        for class_ in reversed(cls.__mro__):
            class_dict = class_.__dict__
            field_annotations.update(class_dict.get("__annotations__", {}))

            if "model_fields" in class_dict:
                # This means the class dict has already been processed by Pydantic
                # All fields should properly be populated in model_fields
                field_values.update(class_dict["model_fields"])
            else:
                field_values.update(class_dict)

        for field_name in field_annotations:
            if not cls._is_entity_type_field(field_values[field_name]):
                cls._add_validator(field_name)

    @classmethod
    def _add_validator(cls, field_name: str):
        def no_op_validator(cls, value, handler):
            return handler(value)

        setattr(
            cls,
            f"template_validate_random_name_{field_name}",
            field_validator_allowing_templates(field_name, mode="wrap")(
                no_op_validator
            ),
        )

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


def EntityTypeField(*args, **kwargs):  # noqa N802
    return Field(is_type_field=True, *args, **kwargs)


def IdentifierField(*args, **kwargs):  # noqa N802
    return Field(max_length=254, pattern=IDENTIFIER_NO_LENGTH, *args, **kwargs)
