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
from typing import Generic, List, Optional, TypeVar, Union

from pydantic import Field, PrivateAttr
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.identifier_model import Identifier
from snowflake.cli.api.project.schemas.native_app.application import (
    PostDeployHook,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
    UpdatableModel,
)


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


class DefaultsField(UpdatableModel):
    schema_: Optional[str] = Field(
        title="Schema.",
        alias="schema",
        default=None,
    )
    stage: Optional[str] = Field(
        title="Stage.",
        default=None,
    )


class EntityModelBase(ABC, UpdatableModel):
    @classmethod
    def get_type(cls) -> str:
        return cls.model_fields["type"].annotation.__args__[0]

    meta: Optional[MetaField] = Field(title="Meta fields", default=None)
    identifier: Optional[Union[Identifier | str]] = Field(
        title="Entity identifier", default=None
    )
    _key: Optional[str] = PrivateAttr(default=None)

    def set_key(self, value: str):
        self._key = value

    def validate_identifier(self):
        """Helper that's used by ProjectDefinition validator."""
        if not self._key and not self.identifier:
            raise ValueError("Missing entity identifier")

    @property
    def fqn(self) -> FQN:
        if isinstance(self.identifier, str):
            return FQN.from_string(self.identifier)
        if isinstance(self.identifier, Identifier):
            return FQN.from_identifier_model_v2(self.identifier)
        if self._key:
            return FQN.from_string(self._key)

    @property
    def name(self) -> str:
        return self.fqn.identifier


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
