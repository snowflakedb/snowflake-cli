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

from typing import Any, List, Literal, Optional, Union

import typer
from click import ClickException
from pydantic import BaseModel, ConfigDict, Field
from snowflake.cli.api.exceptions import InvalidTemplate
from snowflake.cli.api.secure_path import SecurePath


class TemplateVariable(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str = Field(..., title="Variable identifier")
    type: Optional[Literal["string", "float", "int"]] = Field(  # noqa: A003
        title="Type of the variable", default=None
    )
    prompt: Optional[str] = Field(title="Prompt message for the variable", default=None)
    default: Optional[Any] = Field(title="Default value of the variable", default=None)

    @property
    def python_type(self):
        # override "unchecked type" (None) with 'str', as Typer deduces type from the value of 'default'
        return {
            "string": str,
            "float": float,
            "int": int,
            None: str,
        }[self.type]

    def prompt_user_for_value(self, no_interactive: bool) -> Union[str, float, int]:
        if no_interactive:
            if not self.default:
                raise ClickException(f"Cannot determine value of variable {self.name}")
            return self.default

        prompt = self.prompt if self.prompt else self.name
        return typer.prompt(prompt, default=self.default, type=self.python_type)


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

    def merge(self, other: Template):
        if not isinstance(other, Template):
            raise ClickException(f"Can not merge template with {type(other)}")

        errors = []
        if self.minimum_cli_version != other.minimum_cli_version:
            errors.append(
                f"minimum_cli_versions do not match: {self.minimum_cli_version} != {other.minimum_cli_version}"
            )
        variable_map = {variable.name: variable for variable in self.variables}
        for other_variable in other.variables:
            if self_variable := variable_map.get(other_variable.name):
                for attr in ["type", "prompt", "default"]:
                    if getattr(self_variable, attr) != getattr(other_variable, attr):
                        errors.append(
                            f"Conflicting variable definitions: '{self_variable.name}' has different values for attribute '{attr}': '{getattr(self_variable, attr)}' != '{getattr(other_variable, attr)}'"
                        )
        if errors:
            error_str = "\n\t" + "\n\t".join(error for error in errors)
            raise ClickException(
                f"Could not merge templates. Following errors found:{error_str}"
            )
        self.files_to_render = list(set(self.files_to_render + other.files_to_render))
        self.variables = list(set(self.variables + other.variables))
        return self

    def _validate_files_exist(self, template_root: SecurePath) -> None:
        for path_in_template in self.files_to_render:
            full_path = template_root / path_in_template
            if not full_path.exists():
                raise InvalidTemplate(
                    f"[files_to_render] contains not-existing file: {path_in_template}"
                )
            if full_path.is_dir():
                raise InvalidTemplate(
                    f"[files_to_render] contains a dictionary: {path_in_template}"
                )
