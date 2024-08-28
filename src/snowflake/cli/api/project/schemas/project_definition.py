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

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from packaging.version import Version
from pydantic import Field, ValidationError, field_validator, model_validator
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.entities.application_entity_model import (
    ApplicationEntityModel,
)
from snowflake.cli.api.project.schemas.entities.common import (
    DefaultsField,
    TargetField,
)
from snowflake.cli.api.project.schemas.entities.entities import (
    EntityModel,
    v2_entity_model_types_map,
)
from snowflake.cli.api.project.schemas.native_app.native_app import (
    NativeApp,
    NativeAppV11,
)
from snowflake.cli.api.project.schemas.snowpark.snowpark import Snowpark
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel
from snowflake.cli.api.utils.types import Context
from typing_extensions import Annotated

AnnotatedEntity = Annotated[EntityModel, Field(discriminator="type")]


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
    entities: Dict[str, AnnotatedEntity] = Field(title="Entity definitions.")

    @model_validator(mode="before")
    @classmethod
    def apply_defaults(cls, data: Dict) -> Dict:
        """
        Applies default values that exist on the model but not specified in yml
        """
        if "defaults" in data and "entities" in data:
            for key, entity in data["entities"].items():
                entity_fields = get_allowed_fields_for_entity(entity)
                if not entity_fields:
                    continue
                for default_key, default_value in data["defaults"].items():
                    if default_key in entity_fields and default_key not in entity:
                        entity[default_key] = default_value
        return data

    @field_validator("entities", mode="after")
    @classmethod
    def validate_entities_identifiers(
        cls, entities: Dict[str, EntityModel]
    ) -> Dict[str, EntityModel]:
        for key, entity in entities.items():
            entity.set_entity_id(key)
            entity.validate_identifier()
        return entities

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

    @classmethod
    def _validate_target_field(
        cls, target_key: str, target_type: EntityModel, entities: Dict[str, EntityModel]
    ):
        if target_key not in entities:
            raise ValueError(f"No such target: {target_key}")

        # Validate the target type
        actual_target_type = entities[target_key].__class__
        if target_type and target_type is not actual_target_type:
            raise ValueError(
                f"Target type mismatch. Expected {target_type.__name__}, got {actual_target_type.__name__}"
            )

    defaults: Optional[DefaultsField] = Field(
        title="Default key/value entity values that are merged recursively for each entity.",
        default=None,
    )

    env: Optional[Dict[str, Union[str, int, bool]]] = Field(
        title="Default environment specification for this project.",
        default=None,
    )

    mixins: Optional[Dict[str, Dict]] = Field(
        title="Mixins to apply to entities",
        default=None,
    )

    @model_validator(mode="before")
    @classmethod
    def apply_mixins(cls, data: Dict) -> Dict:
        """
        Applies mixins to those entities, whose meta field contains the mixin name.
        """
        if "mixins" not in data or "entities" not in data:
            return data

        for entity in data["entities"].values():
            entity_mixins = entity_mixins_to_list(
                entity.get("meta", {}).get("use_mixins")
            )

            entity_fields = get_allowed_fields_for_entity(entity)
            if entity_fields and entity_mixins:
                for mixin_name in entity_mixins:
                    if mixin_name in data["mixins"]:
                        for key, value in data["mixins"][mixin_name].items():
                            if key in entity_fields:
                                entity[key] = value
                    else:
                        raise ValueError(f"Mixin {mixin_name} not found in mixins")
        return data

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
    if entity_type not in v2_entity_model_types_map:
        return []

    entity_model = v2_entity_model_types_map[entity_type]
    return entity_model.model_fields
