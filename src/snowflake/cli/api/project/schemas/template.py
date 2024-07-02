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

from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, Field
from snowflake.cli.api.secure_path import SecurePath


class TemplateVariableType(Enum):
    STRING = "string"
    INTEGER = "int"
    FLOAT = "float"

    @property
    def python_type(self):
        return {
            TemplateVariableType.STRING: str,
            TemplateVariableType.INTEGER: int,
            TemplateVariableType.FLOAT: float,
        }[self]


class TemplateVariable(BaseModel):
    name: str = Field(..., title="Variable identifier")
    type: Optional[TemplateVariableType] = Field(  # noqa: A003
        title="Type of the variable", default=None
    )
    prompt: Optional[str] = Field(title="Prompt message for the variable", default=None)
    default: Optional[Any] = Field(title="Default value of the variable", default=None)


class Template(BaseModel):
    minimum_cli_version: Optional[str] = Field(
        None, title="Minimum version of Snowflake CLI supporting this template"
    )
    files_to_render: List[str] = Field(title="List of files to be rendered", default=[])
    variables: List[TemplateVariable] = Field(
        title="List of variables to be rendered", default=[]
    )

    def __init__(self, template_root: SecurePath, **kwargs):
        super().__init__(**kwargs)
        self._validate_files_exist(template_root)

    def _validate_files_exist(self, template_root: SecurePath) -> None:
        for path_in_template in self.files_to_render:
            full_path = template_root / path_in_template
            if not full_path.exists():
                raise FileNotFoundError(
                    f"Template does not have file {path_in_template}"
                )
            if full_path.is_dir():
                raise IsADirectoryError(
                    f"Template file {path_in_template} is a directory"
                )
