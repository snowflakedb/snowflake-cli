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

from collections import defaultdict
from dataclasses import dataclass
from types import UnionType
from typing import Any, Dict, List, Optional, Union, get_args, get_origin

from packaging.version import Version
from pydantic import Field, ValidationError, field_validator, model_validator
from pydantic_core.core_schema import ValidationInfo
from snowflake.cli._plugins.nativeapp.entities.application import ApplicationEntityModel
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageChildrenTypes,
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.entities.common import (
    TargetField,
)
from snowflake.cli.api.project.schemas.entities.entities import (
    EntityModel,
    v2_entity_model_types_map,
)
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel
from snowflake.cli.api.project.schemas.v1.native_app.native_app import (
    NativeApp,
    NativeAppV11,
)
from snowflake.cli.api.project.schemas.v1.snowpark.snowpark import Snowpark
from snowflake.cli.api.project.schemas.v1.streamlit.streamlit import Streamlit
from snowflake.cli.api.utils.types import Context
from typing_extensions import Annotated

AnnotatedEntity = Annotated[EntityModel, Field(discriminator="type")]
scalar = str | int | float | bool


@dataclass
class ProjectProperties:
    """
    This class stores 2 objects representing the snowflake project:

    The project_context object:
    - Used as the context for templating when users reference variables in the project definition file.

    The project_definition object:
    - This is a transformed object type through Pydantic, which has been normalized.
    - This object could have slightly different structure than what the users see in their yaml project definition files.
    - This should be used for the business logic of snow CLI modules.
    """

    project_definition: ProjectDefinition
    project_context: Context


@dataclass
class YamlOverride:
    data: dict | list


class _ProjectDefinitionBase(UpdatableModel):
    def __init__(self, *args, **kwargs):
        try:
            super().__init__(**kwargs)
        except ValidationError as e:
            raise SchemaValidationError(e) from e

    definition_version: Union[str, int] = Field(
        title="Version of the project definition schema, which is currently 1",
    )

    @field_validator("definition_version")
    @classmethod
    def _is_supported_version(cls, version: str) -> str:
        version = str(version)
        version_map = get_version_map()
        if version not in version_map:
            raise ValueError(
                f'Version {version} is not supported. Supported versions: {", ".join(version_map)}'
            )
        return version

    def meets_version_requirement(self, required_version: str) -> bool:
        return Version(self.definition_version) >= Version(required_version)


class DefinitionV10(_ProjectDefinitionBase):
    native_app: Optional[NativeApp] = Field(
        title="Native app definitions for the project", default=None
    )
    snowpark: Optional[Snowpark] = Field(
        title="Snowpark functions and procedures definitions for the project",
        default=None,
    )
    streamlit: Optional[Streamlit] = Field(
        title="Streamlit definitions for the project", default=None
    )


class DefinitionV11(DefinitionV10):
    native_app: Optional[NativeAppV11] = Field(
        title="Native app definitions for the project", default=None
    )
    env: Optional[Dict[str, Union[str, int, bool]]] = Field(
        title="Default environment specification for this project.",
        default=None,
    )


class DefinitionV20(_ProjectDefinitionBase):
    entities: Dict[str, AnnotatedEntity] = Field(
        title="Entity definitions.", default={}
    )
    env: Optional[Dict[str, Union[str, int, bool]]] = Field(
        title="Default environment specification for this project.",
        default=None,
    )
    mixins: Optional[Dict[str, Dict]] = Field(
        title="Mixins to apply to entities",
        default=None,
    )

    @model_validator(mode="after")
    def validate_entities_identifiers(self):
        for key, entity in self.entities.items():
            entity.set_entity_id(key)
            entity.validate_identifier()
        return self

    @field_validator("entities", mode="after")
    @classmethod
    def validate_entities(
        cls, entities: Dict[str, AnnotatedEntity]
    ) -> Dict[str, AnnotatedEntity]:
        for key, entity in entities.items():
            # TODO Automatically detect TargetFields to validate
            if isinstance(entity, list):
                for e in entity:
                    cls._validate_single_entity(e, entities)
            else:
                cls._validate_single_entity(entity, entities)
        return entities

    @classmethod
    def _validate_single_entity(
        cls, entity: EntityModel, entities: Dict[str, AnnotatedEntity]
    ):
        if entity.type == ApplicationEntityModel.get_type():
            if isinstance(entity.from_, TargetField):
                target_key = entity.from_.target
                target_object = entity.from_
                target_type = target_object.get_type()
                cls._validate_target_field(target_key, target_type, entities)
        elif entity.type == ApplicationPackageEntityModel.get_type():
            for child_entity in entity.children:
                target_key = child_entity.target
                cls._validate_target_field(
                    target_key, ApplicationPackageChildrenTypes, entities
                )

    @classmethod
    def _validate_target_field(
        cls, target_key: str, target_type: EntityModel, entities: Dict[str, EntityModel]
    ):
        if target_key not in entities:
            raise ValueError(f"No such target: {target_key}")

        # Validate the target type
        if target_type:
            actual_target_type = entities[target_key].__class__
            if get_origin(target_type) in (Union, UnionType):
                if actual_target_type not in get_args(target_type):
                    expected_types_str = ", ".join(
                        [t.__name__ for t in get_args(target_type)]
                    )
                    raise ValueError(
                        f"Target type mismatch. Expected one of [{expected_types_str}], got {actual_target_type.__name__}"
                    )
            elif target_type is not actual_target_type:
                raise ValueError(
                    f"Target type mismatch. Expected {target_type.__name__}, got {actual_target_type.__name__}"
                )

    @model_validator(mode="before")
    @classmethod
    def apply_mixins(cls, data: Dict, info: ValidationInfo) -> Dict:
        """
        Applies mixins to those entities, whose meta field contains the mixin name.
        """
        if "mixins" not in data or "entities" not in data:
            return data

        duplicated_run = (
            info.context.get("is_duplicated_run", False) if info.context else False
        )
        if not duplicated_run:
            entities = data["entities"]
            for entity_name, entity in entities.items():
                entity_mixins = entity_mixins_to_list(
                    entity.get("meta", {}).get("use_mixins")
                )

                merged_values = cls._merge_mixins_with_entity(
                    entity_id=entity_name,
                    entity=entity,
                    entity_mixins_names=entity_mixins,
                    mixin_defs=data["mixins"],
                )
                entities[entity_name] = merged_values

        return data

    @classmethod
    def _merge_mixins_with_entity(
        cls,
        entity_id: str,
        entity: dict,
        entity_mixins_names: list,
        mixin_defs: dict,
    ) -> dict:
        # Validate mixins
        for mixin_name in entity_mixins_names:
            if mixin_name not in mixin_defs:
                raise ValueError(f"Mixin {mixin_name} not defined")

        # Build object override data from mixins
        data: dict = {}
        for mx_name in entity_mixins_names:
            data = cls._merge_data(data, mixin_defs[mx_name])

        for key, override_value in data.items():
            if key not in get_allowed_fields_for_entity(entity):
                raise ValueError(
                    f"Unsupported key '{key}' for entity {entity_id} of type {entity['type']} "
                )

            entity_value = entity.get(key)
            if (
                entity_value is not None
                and not isinstance(entity_value, YamlOverride)
                and not isinstance(entity_value, type(override_value))
            ):
                raise ValueError(
                    f"Value from mixins for property {key} is of type '{type(override_value).__name__}' "
                    f"while entity {entity_id} expects value of type '{type(entity_value).__name__}'"
                )

        # Apply entity data on top of mixins
        data = cls._merge_data(data, entity)
        return data

    @model_validator(mode="after")
    def validate_dependencies(self):
        """
        Checks if entities listed in depends_on section exist in the project
        """
        missing_dependencies = defaultdict(list)
        for entity_id, entity in self.entities.items():
            if entity.meta:
                for dependency in entity.meta.depends_on:
                    if dependency not in self.entities:
                        missing_dependencies[entity_id].append(dependency)

        if missing_dependencies:
            raise ValueError(_get_missing_dependencies_message(missing_dependencies))

    @classmethod
    def _merge_data(
        cls,
        left: dict | list | scalar | None,
        right: dict | list | scalar | None | YamlOverride,
    ):
        """
        Merges right data into left. Right and left is expected to be of the same type, if not right is returned.
        If left is sequence then missing elements from right are appended.
        If left is dictionary then we update it with data from right. The update is done recursively key by key.
        """
        if isinstance(right, YamlOverride):
            return right.data

        if left is None:
            return right

        # At that point left and right are of the same type
        if isinstance(left, dict) and isinstance(right, dict):
            data = dict(left)
            for key in right:
                data[key] = cls._merge_data(left=data.get(key), right=right[key])
            return data

        if isinstance(left, list) and isinstance(right, list):
            return _unique_extend(left, right)

        if not isinstance(right, type(left)):
            raise ValueError(f"Could not merge {type(right)} and {type(left)}.")

        return right

    def get_entities_by_type(self, entity_type: str):
        return {i: e for i, e in self.entities.items() if e.get_type() == entity_type}


def build_project_definition(**data) -> ProjectDefinition:
    """
    Returns a ProjectDefinition instance with a version matching the provided definition_version value
    """
    if not isinstance(data, dict):
        return
    version = data.get("definition_version")
    version_model = get_version_map().get(str(version))
    if not version or not version_model:
        # Raises a SchemaValidationError
        _ProjectDefinitionBase(**data)
    return version_model(**data)


ProjectDefinitionV1 = Union[DefinitionV10, DefinitionV11]
ProjectDefinitionV2 = DefinitionV20
ProjectDefinition = Union[ProjectDefinitionV1, ProjectDefinitionV2]


def get_version_map():
    version_map = {"1": DefinitionV10, "1.1": DefinitionV11, "2": DefinitionV20}
    return version_map


def entity_mixins_to_list(entity_mixins: Optional[str | List[str]]) -> List[str]:
    """
    Convert an optional string or a list of strings to a list of strings.
    """
    if entity_mixins is None:
        return []
    if isinstance(entity_mixins, str):
        return [entity_mixins]
    return entity_mixins


def get_allowed_fields_for_entity(entity: Dict[str, Any]) -> List[str]:
    """
    Get the allowed fields for the given entity.
    """
    entity_type = entity.get("type")
    if entity_type is None:
        raise ValueError("Entity is missing type declaration.")

    if entity_type not in v2_entity_model_types_map:
        return []

    entity_model = v2_entity_model_types_map[entity_type]
    return entity_model.model_fields


def _unique_extend(list_a: List, list_b: List) -> List:
    new_list = list(list_a)
    for item in list_b:
        if all(item != x for x in list_a):
            new_list.append(item)
    return new_list


def _get_missing_dependencies_message(
    missing_dependencies: Dict[str, List[str]]
) -> str:
    missing_dependencies_message = []
    for entity_id, dependencies in missing_dependencies.items():
        missing_dependencies_message.append(
            f"\n Entity {entity_id} depends on non-existing entities: {', '.join(dependencies)}"
        )
    return "".join(missing_dependencies_message)
