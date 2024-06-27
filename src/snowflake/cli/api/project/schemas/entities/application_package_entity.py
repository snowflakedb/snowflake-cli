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
    PostDeployField,
    PostDeployFieldType,
    WarehouseField,
    WarehouseFieldType,
)
from snowflake.cli.api.project.schemas.native_app.package import DistributionOptions
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
    UpdatableModel,
)


class ApplicationPackageEntity(UpdatableModel):
    entity_type: str = "application package"
    name: str = Field(
        title="Name of the application package created when this entity is deployed"
    )
    artifacts: List[Union[PathMapping, str]] = Field(
        title="List of file source and destination pairs to add to the deploy root",
    )
    deploy_root: Optional[str] = Field(
        title="Folder at the root of your project where the build step copies the artifacts",
        default="output/deploy/",
    )
    stage: Optional[str] = Field(
        title="Identifier of the stage that stores the application artifacts.",
        default="app_src.stage",
    )
    distribution: Optional[DistributionOptions] = Field(
        title="Distribution of the application package created by the Snowflake CLI",
        default="internal",
    )
    manifest: str = Field(
        title="Path to manifest.yml",
    )
    meta: Optional[ApplicationPackageMetaField] = Field(
        title="Application package meta fields", default=None
    )


class ApplicationPackageMetaField(UpdatableModel):
    warehouse: WarehouseFieldType = WarehouseField
    role: Optional[str] = IdentifierField(
        title="Role to use when creating the application package and provider-side objects",
        default=None,
    )
    post_deploy: PostDeployFieldType = PostDeployField
