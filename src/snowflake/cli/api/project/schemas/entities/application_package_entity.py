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

from typing import List, Optional, Union

from pydantic import Field
from snowflake.cli.api.project.schemas.entities.common_properties import (
    GrantEntityProperty,
)
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.schemas.updatable_model import (
    UpdatableModel,
)


class ApplicationPackageEntity(UpdatableModel):
    entity_type: str = "application package"
    name: str = Field(
        title="Application package identifier",
    )
    manifest: str = Field(
        title="Path to manifest.yml",
    )
    artifacts: List[Union[PathMapping, str]] = Field(
        title="List of file source and destination pairs to add to the deploy root",
    )
    grant: Optional[List[GrantEntityProperty]] = Field(title="Grants", default=None)
