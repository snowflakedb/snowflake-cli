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

from typing import Literal, Optional

from pydantic import Field, field_validator
from snowflake.cli.api.project.schemas.entities.application_package_entity_model import (
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBase,
    TargetField,
)
from snowflake.cli.api.project.schemas.identifier_model import Identifier
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
)
from snowflake.cli.api.project.util import append_test_resource_suffix


class ApplicationEntityModel(EntityModelBase):
    type: Literal["application"] = DiscriminatorField()  # noqa A003
    from_: TargetField[ApplicationPackageEntityModel] = Field(
        alias="from",
        title="An application package this entity should be created from",
    )
    debug: Optional[bool] = Field(
        title="Whether to enable debug mode when using a named stage to create an application object",
        default=None,
    )

    @field_validator("identifier")
    @classmethod
    def append_test_resource_suffix_to_identifier(
        cls, input_value: Identifier | str
    ) -> Identifier | str:
        identifier = (
            input_value.name if isinstance(input_value, Identifier) else input_value
        )
        with_suffix = append_test_resource_suffix(identifier)
        if isinstance(input_value, Identifier):
            return input_value.model_copy(update=dict(name=with_suffix))
        return with_suffix
