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
        """
        Pydantic provides 2 options to pass in context:
        1) Through `model_validate()` as a second argument.
        2) Through a custom init method and the use of ContextVar

        We decided not to use 1) because it silently stops working
        if someone adds a pass through __init__ to any of the Pydantic models.

        We decided to go with 2) as the safer approach.
        Calling validate_python() in the __init__ is how we can pass context
        on initialization according to Pydantic's documentation:
        https://docs.pydantic.dev/latest/concepts/validators/#using-validation-context-with-basemodel-initialization
        """
        self.__pydantic_validator__.validate_python(
            data,
            self_instance=self,
            context=_initial_context.get(),
        )

    @classmethod
    def _is_entity_type_field(cls, field: Any) -> bool:
        """
        Checks if a field is of type `DiscriminatorField`
        """
        if not isinstance(field, FieldInfo) or not field.json_schema_extra:
            return False

        return (
            "is_discriminator_field" in field.json_schema_extra
            and field.json_schema_extra["is_discriminator_field"]
        )

    @classmethod
    def __init_subclass__(cls, **kwargs):
        """
        This method will collect all the Pydantic annotations for the class
        currently being initialized (any subclass of `UpdatableModel`).

        It will add a field validator wrapper for every Pydantic field
        in order to skip validation when templates are found.

        It will apply this to all Pydantic fields, except for fields
        marked as `DiscriminatorField`. These will be skipped because
        Pydantic does not support validators for discriminator field types.
        """

        super().__init_subclass__(**kwargs)

        field_annotations = {}
        field_values = {}
        # Go through the inheritance classes and collect all the annotations and
        # all the values of the class attributes. We go in reverse order so that
        # values in subclasses overrides values from parent classes in case of field overrides.

        private_attrs = set()
        for class_ in reversed(cls.__mro__):
            class_dict = class_.__dict__
            field_annotations.update(class_dict.get("__annotations__", {}))

            if "model_fields" in class_dict and class_.model_fields:
                # This means the class dict has already been processed by Pydantic
                # All fields should properly be populated in model_fields
                field_values.update(class_.model_fields)
            else:
                # If Pydantic did not process this class yet, get the values from class_dict directly
                field_values.update(class_dict)
            for pa in class_dict.get("__private_attributes__", []):
                private_attrs.add(pa)

        # Add Pydantic validation wrapper around all fields except `DiscriminatorField`s
        for field_name in field_annotations:
            if field_name in private_attrs:
                continue
            field = field_values.get(field_name)
            if not cls._is_entity_type_field(field):
                cls._add_validator(field_name)

    @classmethod
    def _add_validator(cls, field_name: str):
        """
        Adds a Pydantic validator with mode=wrap for the provided `field_name`.
        During validation, this will check if the field is templated (not expanded yet)
        and in that case, it will skip all the remaining Pydantic validation on that field.

        Since this validator is added last, it will skip all the other field validators
        defined in the subclasses when templates are found.

        This logic on templates only applies when context contains `skip_validation_on_templates` flag.
        """

        def validator_skipping_templated_str(cls, value, handler, info: ValidationInfo):
            if _is_templated(info, value):
                return value
            return handler(value)

        setattr(
            cls,
            # Unique name so that subclasses get a unique instance of this validator
            f"_{cls.__module__}.{cls.__name__}_validate_{field_name}",
            field_validator(field_name, mode="wrap")(validator_skipping_templated_str),
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


def DiscriminatorField(*args, **kwargs):  # noqa N802
    """
    Use this type for discriminator fields used for differentiating
    between different entity types.

    When this `DiscriminatorField` is used on a pydantic attribute,
    we will not allow templating on it.
    """
    extra = dict(is_discriminator_field=True)
    return Field(json_schema_extra=extra, *args, **kwargs)


def IdentifierField(*args, **kwargs):  # noqa N802
    return Field(max_length=254, pattern=IDENTIFIER_NO_LENGTH, *args, **kwargs)
