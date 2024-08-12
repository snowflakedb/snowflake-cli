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

from pydantic import Field
from snowflake.cli.api.project.schemas.entities.application_package_entity_model import (
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBase,
    TargetField,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
)


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
