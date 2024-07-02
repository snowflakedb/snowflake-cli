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

from typing import List, Optional

from pydantic import Field, GetCoreSchemaHandler, ValidationInfo
from pydantic_core import core_schema
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
)

WarehouseFieldType = Optional[str]
WarehouseField: WarehouseFieldType = IdentifierField(
    title="Warehouse used to run the scripts", default=None
)

PostDeployFieldType = Optional[List[str]]
PostDeployField: PostDeployFieldType = Field(
    title="List of SQL file paths relative to the project root", default=None
)


class TargetField:
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return self.value

    @classmethod
    def validate(cls, value: str, info: ValidationInfo):
        return cls(value)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.with_info_after_validator_function(
            cls.validate, handler(str), field_name=handler.field_name
        )
