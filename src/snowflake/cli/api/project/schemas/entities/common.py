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

from abc import ABC
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

from pydantic import Field, PrivateAttr, field_validator
from pydantic_core.core_schema import ValidationInfo
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
    UpdatableModel,
)


class SqlScriptHookType(UpdatableModel):
    sql_script: str = Field(title="SQL file path relative to the project root")

    # Used to store a user-friendly path for this script, when the
    # value of `sql_script` is a path to a different file
    # This is used in the UI to display the path relative to the
    # project root when `sql_script` is a actually path to a temp file
    # generated by the in-memory PDF v1 to v2 conversion
    _display_path: str = PrivateAttr(default="")

    @property
    def display_path(self):
        return self._display_path or self.sql_script


# Currently sql_script is the only supported hook type. Change to a Union once other hook types are added
PostDeployHook = SqlScriptHookType


class MetaField(UpdatableModel):
    warehouse: Optional[str] = IdentifierField(
        title="Warehouse used to run the scripts", default=None
    )
    role: Optional[str] = IdentifierField(
        title="Role to use when creating the entity object",
        default=None,
    )
    post_deploy: Optional[List[PostDeployHook]] = Field(
        title="Actions that will be executed after the application object is created/upgraded",
        default=None,
    )
    use_mixins: Optional[List[str]] = Field(
        title="Name of the mixin used to fill the entity fields",
        default=None,
    )

    depends_on: Optional[List[str]] = Field(
        title="Entities that need to be deployed before this one", default_factory=list
    )

    action_arguments: Optional[Dict[str, Dict[str, Union[int, bool, str]]]] = Field(
        title="Arguments that will be used, when this entity is called as a dependency of other entity",
        default_factory=dict,
    )

    @field_validator("use_mixins", mode="before")
    @classmethod
    def ensure_use_mixins_is_a_list(
        cls, mixins: Optional[str | List[str]]
    ) -> Optional[List[str]]:
        if isinstance(mixins, str):
            return [mixins]
        return mixins

    @field_validator("action_arguments", mode="before")
    @classmethod
    def arguments_validator(cls, arguments: Dict, info: ValidationInfo) -> Dict:
        duplicated_run = (
            info.context.get("is_duplicated_run", False) if info.context else False
        )
        if not duplicated_run:
            for argument_dict in arguments.values():
                for k, v in argument_dict.items():
                    argument_dict[k] = cls._cast_value(v)

        return arguments

    @staticmethod
    def _cast_value(value: str) -> Union[int, bool, str]:
        if value.lower() in ["true", "false"]:
            return value.lower() == "true"

        try:
            return int(value)
        except ValueError:
            return value

    def __eq__(self, other):
        return self.entity_id == other.entity_id

    def __hash__(self):
        return hash(self.entity_id)


class Identifier(UpdatableModel):
    name: str = Field(title="Entity name")
    schema_: Optional[str] = Field(title="Entity schema", alias="schema", default=None)
    database: Optional[str] = Field(title="Entity database", default=None)


class EntityModelBase(ABC, UpdatableModel):
    @classmethod
    def get_type(cls) -> str:
        return cls.model_fields["type"].annotation.__args__[0]

    meta: Optional[MetaField] = Field(title="Meta fields", default=None)
    identifier: Optional[Union[Identifier | str]] = Field(
        title="Entity identifier", default=None
    )
    # Set by parent model in post validation. To reference it use `entity_id`.
    _entity_id: str = PrivateAttr(default=None)

    @property
    def entity_id(self):
        return self._entity_id

    def set_entity_id(self, value: str):
        self._entity_id = value

    def validate_identifier(self):
        """Helper that's used by ProjectDefinition validator."""
        if not self._entity_id and not self.identifier:
            raise ValueError("Missing entity identifier")

    @property
    def fqn(self) -> FQN:
        if isinstance(self.identifier, str):
            return FQN.from_string(self.identifier)
        if isinstance(self.identifier, Identifier):
            return FQN.from_identifier_model_v2(self.identifier)
        if self.entity_id:
            return FQN.from_string(self.entity_id)


TargetType = TypeVar("TargetType")


class TargetField(UpdatableModel, Generic[TargetType]):
    target: str = Field(
        title="Reference to a target entity",
    )

    def get_type(self) -> type:
        """
        Returns the generic type of this class, indicating the entity type.
        Pydantic extracts Generic annotations, and populates
        them in __pydantic_generic_metadata__
        """
        return self.__pydantic_generic_metadata__["args"][0]


class ImportsBaseModel:
    imports: Optional[List[str]] = Field(
        title="Stage and path to previously uploaded files you want to import",
        default=[],
    )

    def get_imports_sql(self) -> str | None:
        if not self.imports:
            return None
        imports = ", ".join(f"'{i}'" for i in self.imports)
        return f"IMPORTS = ({imports})"


class ExternalAccessBaseModel:
    external_access_integrations: Optional[List[str]] = Field(
        title="Names of external access integrations needed for this entity to access external networks",
        default=[],
    )
    secrets: Optional[Dict[str, str]] = Field(
        title="Assigns the names of secrets to variables so that you can use the variables to reference the secrets",
        default={},
    )

    def get_external_access_integrations_sql(self) -> str | None:
        if not self.external_access_integrations:
            return None
        external_access_integration_name = ", ".join(self.external_access_integrations)
        return f"external_access_integrations=({external_access_integration_name})"

    def get_secrets_sql(self) -> str | None:
        if not self.secrets:
            return None
        secrets = ", ".join(f"'{key}'={value}" for key, value in self.secrets.items())
        return f"secrets=({secrets})"


class ProcessorMapping(UpdatableModel):
    name: str = Field(
        title="Name of a processor to invoke on a collection of artifacts."
    )
    properties: Optional[Dict[str, Any]] = Field(
        title="A set of key-value pairs used to configure the output of the processor. Consult a specific processor's documentation for more details on the supported properties.",
        default=None,
    )


class PathMapping(UpdatableModel):
    src: str = Field(
        title="Source path or glob pattern (relative to project root)", default=None
    )

    dest: Optional[str] = Field(
        title="Destination path on stage",
        description="Paths are relative to stage root; paths ending with a slash indicate that the destination is a directory which source files should be copied into.",
        default=None,
    )

    processors: Optional[List[Union[str, ProcessorMapping]]] = Field(
        title="List of processors to apply to matching source files during bundling.",
        default=[],
    )

    @field_validator("processors")
    @classmethod
    def transform_processors(
        cls, input_values: Optional[List[Union[str, Dict, ProcessorMapping]]]
    ) -> List[ProcessorMapping]:
        if input_values is None:
            return []

        transformed_processors: List[ProcessorMapping] = []
        for input_processor in input_values:
            if isinstance(input_processor, str):
                transformed_processors.append(ProcessorMapping(name=input_processor))
            elif isinstance(input_processor, Dict):
                transformed_processors.append(ProcessorMapping(**input_processor))
            else:
                transformed_processors.append(input_processor)
        return transformed_processors


Artifacts = List[Union[PathMapping, str]]


class EntityModelBaseWithArtifacts(EntityModelBase):
    artifacts: Artifacts = Field(
        title="List of paths or file source/destination pairs to add to the deploy root",
    )
    deploy_root: Optional[str] = Field(
        title="Folder at the root of your project where the build step copies the artifacts",
        default="output/deploy/",
    )

    @field_validator("artifacts")
    @classmethod
    def transform_artifacts(cls, orig_artifacts: Artifacts) -> List[PathMapping]:
        transformed_artifacts: List[PathMapping] = []
        if orig_artifacts is None:
            return transformed_artifacts

        for artifact in orig_artifacts:
            if isinstance(artifact, PathMapping):
                transformed_artifacts.append(artifact)
            else:
                transformed_artifacts.append(PathMapping(src=artifact))

        return transformed_artifacts
