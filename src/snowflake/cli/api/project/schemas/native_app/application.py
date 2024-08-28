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

from pydantic import Field, field_validator
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
    UpdatableModel,
)
from snowflake.cli.api.project.util import append_test_resource_suffix


class Application(UpdatableModel):
    role: Optional[str] = Field(
        title="Role to use when creating the application object and consumer-side objects",
        default=None,
    )
    name: Optional[str] = Field(
        title="Name of the application object created when you run the snow app run command",
        default=None,
    )
    warehouse: Optional[str] = IdentifierField(
        title="Name of the application object created when you run the snow app run command",
        default=None,
    )
    debug: Optional[bool] = Field(
        title="When set, forces debug_mode on/off for the deployed application object",
        default=None,
    )
    post_deploy: Optional[List[PostDeployHook]] = Field(
        title="Actions that will be executed after the application object is created/upgraded",
        default=None,
    )

    @field_validator("name")
    @classmethod
    def append_test_resource_suffix_to_name(cls, input_value: str) -> str:
        return append_test_resource_suffix(input_value)


class ApplicationV11(Application):
    # Templated defaults only supported in v1.1+
    name: Optional[str] = Field(
        title="Name of the application object created when you run the snow app run command",
        default="<% fn.concat_ids(ctx.native_app.name, '_', fn.sanitize_id(fn.get_username('unknown_user')) | lower) %>",
    )
