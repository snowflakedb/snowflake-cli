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

from typing import List, Literal, Optional

from pydantic import Field, field_validator, model_validator
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
    UpdatableModel,
)
from snowflake.cli.api.project.util import append_test_resource_suffix

DistributionOptions = Literal["internal", "external", "INTERNAL", "EXTERNAL"]


class Package(UpdatableModel):
    scripts: Optional[List[str]] = Field(
        title="List of SQL file paths relative to the project root", default=None
    )
    role: Optional[str] = IdentifierField(
        title="Role to use when creating the application package and provider-side objects",
        default=None,
    )
    name: Optional[str] = IdentifierField(
        title="Name of the application package created when you run the snow app run command",
        default=None,
    )
    warehouse: Optional[str] = IdentifierField(
        title="Warehouse used to run the scripts", default=None
    )
    distribution: Optional[DistributionOptions] = Field(
        title="Distribution of the application package created by the Snowflake CLI",
        default="internal",
    )
    post_deploy: Optional[List[PostDeployHook]] = Field(
        title="Actions that will be executed after the application package object is created/updated",
        default=None,
    )

    @field_validator("name")
    @classmethod
    def append_test_resource_suffix_to_name(cls, input_value: str) -> str:
        return append_test_resource_suffix(input_value)

    @field_validator("scripts")
    @classmethod
    def validate_scripts(cls, input_list):
        if len(input_list) != len(set(input_list)):
            raise ValueError(
                "package.scripts field should contain unique values. Check the list for duplicates and try again"
            )
        return input_list

    @model_validator(mode="after")
    @classmethod
    def validate_no_scripts_and_post_deploy(cls, value: Package):
        if value.scripts and value.post_deploy:
            raise ValueError(
                "package.scripts and package.post_deploy fields cannot be used together. "
                "We recommend using package.post_deploy for all post package deploy scripts"
            )
        return value


class PackageV11(Package):
    # Templated defaults only supported in v1.1+
    name: Optional[str] = IdentifierField(
        title="Name of the application package created when you run the snow app run command",
        default="<% fn.concat_ids(ctx.native_app.name, '_pkg_', fn.sanitize_id(fn.get_username('unknown_user')) | lower) %>",
    )
