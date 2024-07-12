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
from typing import Dict, Optional, Union

from packaging.version import Version
from pydantic import Field, ValidationError, field_validator, model_validator
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.entities.application_entity import (
    ApplicationEntity,
)
from snowflake.cli.api.project.schemas.entities.common import (
    DefaultsField,
    TargetField,
)
from snowflake.cli.api.project.schemas.entities.entities import (
    Entity,
    v2_entity_types_map,
)
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.snowpark.snowpark import Snowpark
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel
from snowflake.cli.api.utils.models import ProjectEnvironment
from snowflake.cli.api.utils.types import Context
from typing_extensions import Annotated


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
    env: Union[Dict[str, str], ProjectEnvironment, None] = Field(
        title="Environment specification for this project.",
        default=None,
        validation_alias="env",
        union_mode="smart",
    )

    @field_validator("env")
    @classmethod
    def _convert_env(
        cls, env: Union[Dict, ProjectEnvironment, None]
    ) -> ProjectEnvironment:
        if isinstance(env, ProjectEnvironment):
            return env
        return ProjectEnvironment(default_env=(env or {}), override_env={})


class DefinitionV20(_ProjectDefinitionBase):
    entities: Dict[str, Annotated[Entity, Field(discriminator="type")]] = Field(
        title="Entity definitions."
    )

    @model_validator(mode="before")
    @classmethod
    def apply_defaults(cls, data: Dict) -> Dict:
        """
        Applies default values that exist on the model but not specified in yml
        """
        if "defaults" in data and "entities" in data:
            for key, entity in data["entities"].items():
                entity_type = entity["type"]
                if entity_type not in v2_entity_types_map:
                    continue
                entity_model = v2_entity_types_map[entity_type]
                for default_key, default_value in data["defaults"].items():
                    if (
                        default_key in entity_model.model_fields
                        and default_key not in entity
                    ):
                        entity[default_key] = default_value
        return data

    @field_validator("entities", mode="after")
    @classmethod
    def validate_entities(cls, entities: Dict[str, Entity]) -> Dict[str, Entity]:
        for key, entity in entities.items():
            # TODO Automatically detect TargetFields to validate
            if entity.type == ApplicationEntity.get_type():
                if isinstance(entity.from_.target, TargetField):
                    target_key = str(entity.from_.target)
                    target_class = entity.from_.__class__.model_fields["target"]
                    target_type = target_class.annotation.__args__[0]
                    cls._validate_target_field(target_key, target_type, entities)
        return entities

    @classmethod
    def _validate_target_field(
        cls, target_key: str, target_type: Entity, entities: Dict[str, Entity]
    ):
        if target_key not in entities:
            raise ValueError(f"No such target: {target_key}")
        else:
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

    env: Union[Dict[str, str], ProjectEnvironment, None] = Field(
        title="Environment specification for this project.",
        default=None,
        validation_alias="env",
        union_mode="smart",
    )

    @field_validator("env")
    @classmethod
    def _convert_env(
        cls, env: Union[Dict, ProjectEnvironment, None]
    ) -> ProjectEnvironment:
        if isinstance(env, ProjectEnvironment):
            return env
        return ProjectEnvironment(default_env=(env or {}), override_env={})


def build_project_definition(**data):
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
    version_map = {"1": DefinitionV10, "1.1": DefinitionV11}
    if FeatureFlag.ENABLE_PROJECT_DEFINITION_V2.is_enabled():
        version_map["2"] = DefinitionV20
    return version_map
