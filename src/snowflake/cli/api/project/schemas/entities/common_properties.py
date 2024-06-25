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

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import (
    UpdatableModel,
)


class GrantTarget(UpdatableModel):
    name: str = Field(title="Name of the target object")
    target_type: str = Field(title="Type of the target object")


class GrantEntityProperty(UpdatableModel):
    privilege: str = Field(title="The privilege to be granted")
    target: GrantTarget = Field(
        title="The object on which the privilege is granted",
    )
