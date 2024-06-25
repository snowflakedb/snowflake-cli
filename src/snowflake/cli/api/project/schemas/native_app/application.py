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

from pydantic import Field, model_validator
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
    UpdatableModel,
)


class ApplicationPostDeployHook(UpdatableModel):
    sql_script: Optional[str] = Field(
        title="SQL file path relative to the project root", default=None
    )

    @model_validator(mode="before")
    @classmethod
    def ensure_exactly_one_hook_type(cls, hook):
        supported_keys = cls.model_fields.keys()
        supported_keys_str = ", ".join(supported_keys)
        if type(hook) == str:
            raise ValueError(
                f"Hooks must be dictionaries with one of the following keys: {supported_keys_str}"
            )
        actual_keys = hook.keys()
        previously_found_key = None
        for key in actual_keys:
            if key in supported_keys:
                if previously_found_key:
                    raise ValueError(
                        f"Only one of the following keys can be specified: {supported_keys_str}"
                    )
                else:
                    previously_found_key = True
        if not previously_found_key:
            raise ValueError(
                f"One of the following keys must be specified: {supported_keys_str}"
            )

        return hook


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
    post_deploy: Optional[List[ApplicationPostDeployHook]] = Field(
        title="Actions that will be executed after the application object is created/upgraded",
        default=None,
    )
