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

from typing import Optional, cast

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
    UpdatableModel,
)


class Identifier(UpdatableModel):
    name: str = Field(title="Entity name")
    schema_: str = Field(title="Entity schema", alias="schema", default=None)
    database: str = Field(title="Entity database", default=None)


class ObjectIdentifierBaseModel:
    """
    Type representing a base class defining object that can be identified by fully qualified name (db.schema.name).
    This is not a Pydantic model and the purpose of this class is to provide typing support to Pydantic models
    generated using a factory class ObjectIdentifierModel.
    """

    name: str
    database: Optional[str]
    schema_name: Optional[str]


def ObjectIdentifierModel(object_name: str) -> ObjectIdentifierBaseModel:  # noqa: N802
    """Generates ObjectIdentifierBaseModel but with object specific descriptions."""

    class _ObjectIdentifierModel(ObjectIdentifierBaseModel):
        name: str = Field(title=f"{object_name.capitalize()} name")
        database: Optional[str] = IdentifierField(
            title=f"Name of the database for the {object_name}", default=None
        )
        schema_name: Optional[str] = IdentifierField(
            title=f"Name of the schema for the {object_name}",
            default=None,
            alias="schema",
        )

    return cast(ObjectIdentifierBaseModel, _ObjectIdentifierModel)
