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

from typing import Optional

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
    UpdatableModel,
)


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
        title="Whether to enable debug mode when using a named stage to create an application object",
        default=True,
    )
