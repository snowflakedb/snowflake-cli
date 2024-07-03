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

from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from packaging.version import Version
from pydantic import Field, ValidationError, field_validator
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.snowpark.snowpark import Snowpark
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel
from snowflake.cli.api.utils.models import ProjectEnvironment
from snowflake.cli.api.utils.types import Context


@dataclass
class ProjectProperties:
    """
    This class stores 2 objects representing the snowflake project:

    The project_context object:
    - Used as the context for templating when users reference variables in the project definition file.

    The project_definition object:
    - This is a transformed object type through Pydantic, which has been normalized.
    - This object could have slightly different structure than what the users see in their yaml project definition files.
    - This should be used for the business logic of snow CLI modules.
    """

    project_definition: ProjectDefinition
    project_context: Context


class _BaseDefinition(UpdatableModel):
    def __init__(self, *args, **kwargs):
        try:
            super().__init__(**kwargs)
        except ValidationError as e:
            raise SchemaValidationError(e) from e

    definition_version: Union[str, int] = Field(
        title="Version of the project definition schema, which is currently 1",
    )

    @field_validator("definition_version")
    @classmethod
    def _is_supported_version(cls, version: str) -> str:
        version = str(version)
        if version not in _version_map:
            raise ValueError(
                f'Version {version} is not supported. Supported versions: {", ".join(_version_map)}'
            )
        return version

    def meets_version_requirement(self, required_version: str) -> bool:
        return Version(self.definition_version) >= Version(required_version)


class _DefinitionV10(_BaseDefinition):
    native_app: Optional[NativeApp] = Field(
        title="Native app definitions for the project", default=None
    )
    snowpark: Optional[Snowpark] = Field(
        title="Snowpark functions and procedures definitions for the project",
        default=None,
    )
    streamlit: Optional[Streamlit] = Field(
        title="Streamlit definitions for the project", default=None
    )


class _DefinitionV11(_DefinitionV10):
    env: Union[Dict[str, str], ProjectEnvironment, None] = Field(
        title="Environment specification for this project.",
        default=None,
        validation_alias="env",
        union_mode="smart",
    )

    @field_validator("env")
    @classmethod
    def _convert_env(
        cls, env: Union[Dict, ProjectEnvironment, None]
    ) -> ProjectEnvironment:
        if isinstance(env, ProjectEnvironment):
            return env
        return ProjectEnvironment(default_env=(env or {}), override_env={})


class ProjectDefinition(_DefinitionV11):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._validate(kwargs)

    @staticmethod
    def _validate(data: Any):
        if not isinstance(data, dict):
            return
        if version := str(data.get("definition_version")):
            version_model = _version_map.get(version)
            if not version_model:
                raise ValueError(
                    f"Unknown schema version: {version}. Supported version: {_supported_version}"
                )
            version_model(**data)


_version_map = {"1": _DefinitionV10, "1.1": _DefinitionV11}
_supported_version = tuple(_version_map.keys())
