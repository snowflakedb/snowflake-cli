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

import logging
from typing import Any, Callable, ClassVar, Dict, List, Literal, Optional, Union

import typer
from click import ClickException
from pydantic import BaseModel, Field, model_validator
from snowflake.cli.api.exceptions import InvalidTemplateError, MissingConfigurationError
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)


def _make_connection_resolver(key: str) -> Callable[[], Optional[str]]:
    def resolver() -> Optional[str]:
        from snowflake.cli.api.config import get_default_connection_dict

        try:
            connection_dict = get_default_connection_dict()
            return connection_dict.get(key)
        except MissingConfigurationError as exc:
            log.warning("Could not resolve connection key '%s': %s", key, exc)
            return None

    return resolver


class TemplateVariable(BaseModel):
    @staticmethod
    def _comma_separated_keys(resolvers: Dict[str, Any]) -> str:
        return ", ".join(f"'{k}'" for k in resolvers)

    _COMPUTED_VALUE_RESOLVERS: ClassVar[Dict[str, Callable[[], Optional[str]]]] = {
        "connection.account": _make_connection_resolver("account"),
        "connection.role": _make_connection_resolver("role"),
    }
    name: str = Field(..., title="Variable identifier")
    type: Optional[Literal["string", "float", "int"]] = Field(  # noqa: A003
        title="Type of the variable", default=None
    )
    prompt: Optional[str] = Field(title="Prompt message for the variable", default=None)
    default: Optional[Any] = Field(title="Default value of the variable", default=None)
    default_computed: Optional[str] = Field(
        title="Compute the default value dynamically. Supported: "
        + _comma_separated_keys(_COMPUTED_VALUE_RESOLVERS),
        default=None,
    )

    @model_validator(mode="after")
    def _validate_defaults_mutual_exclusion(self):
        if self.default is not None and self.default_computed is not None:
            raise InvalidTemplateError(
                f"Variable '{self.name}' has both 'default' and 'default_computed' set. "
                "These are mutually exclusive."
            )
        return self

    @property
    def python_type(self):
        # override "unchecked type" (None) with 'str', as Typer deduces type from the value of 'default'
        return {
            "string": str,
            "float": float,
            "int": int,
            None: str,
        }[self.type]

    def resolve_default(self) -> Optional[Any]:
        """Return effective default: static 'default' or computed 'default_computed'."""
        if self.default is not None:
            return self.default
        if self.default_computed is None:
            return None
        return self._resolve_computed_value(self.default_computed)

    def prompt_user_for_value(self, no_interactive: bool) -> Union[str, float, int]:
        effective_default = self.resolve_default()
        if no_interactive:
            if effective_default is None:
                raise ClickException(f"Cannot determine value of variable {self.name}")
            return effective_default

        prompt = self.prompt if self.prompt else self.name
        return typer.prompt(prompt, default=effective_default, type=self.python_type)

    @staticmethod
    def _resolve_computed_value(key: str) -> Optional[str]:
        resolver = TemplateVariable._COMPUTED_VALUE_RESOLVERS.get(key)
        if resolver is None:
            raise InvalidTemplateError(
                f"Unknown default_computed value: '{key}'. "
                f"Supported values: {TemplateVariable._comma_separated_keys(TemplateVariable._COMPUTED_VALUE_RESOLVERS)}"
            )
        return resolver()


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
                raise InvalidTemplateError(
                    f"[files_to_render] contains not-existing file: {path_in_template}"
                )
            if full_path.is_dir():
                raise InvalidTemplateError(
                    f"[files_to_render] contains a dictionary: {path_in_template}"
                )
