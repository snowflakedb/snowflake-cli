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

from pathlib import Path
from typing import List, Literal, Optional, Union

from pydantic import Field, field_validator
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBase,
    ExternalAccessBaseModel,
)
from snowflake.cli.api.project.schemas.snowpark.argument import Argument
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
    UpdatableModel,
)


class PathMapping(UpdatableModel):
    class Config:
        frozen = True

    src: Path = Field(title="Source path (relative to project root)", default=None)

    dest: Optional[str] = Field(
        title="Destination path on stage",
        description="Paths are relative to stage root; paths ending with a slash indicate that the destination is a directory which source files should be copied into.",
        default=None,
    )


class SnowparkEntityModel(EntityModelBase, ExternalAccessBaseModel):
    handler: str = Field(
        title="Function’s or procedure’s implementation of the object inside source module",
        examples=["functions.hello_function"],
    )
    returns: str = Field(
        title="Type of the result"
    )  # TODO: again, consider Literal/Enum
    signature: Union[str, List[Argument]] = Field(
        title="The signature parameter describes consecutive arguments passed to the object"
    )
    runtime: Optional[Union[str, float]] = Field(
        title="Python version to use when executing ", default=None
    )
    imports: Optional[List[str]] = Field(
        title="Stage and path to previously uploaded files you want to import",
        default=[],
    )
    stage: str = Field(title="Stage in which artifacts will be stored")
    artifacts: List[Union[PathMapping, str]] = Field(title="List of required sources")

    @field_validator("artifacts")
    @classmethod
    def _convert_artifacts(cls, artifacts: Union[dict, str]):
        _artifacts = []
        for artefact in artifacts:
            if isinstance(artefact, PathMapping):
                _artifacts.append(artefact)
            else:
                _artifacts.append(PathMapping(src=artefact))
        return _artifacts

    @field_validator("runtime")
    @classmethod
    def convert_runtime(cls, runtime_input: Union[str, float]) -> str:
        if isinstance(runtime_input, float):
            return str(runtime_input)
        return runtime_input

    @field_validator("artifacts")
    @classmethod
    def validate_artifacts(cls, artifacts: List[Path]) -> List[Path]:
        for artefact in artifacts:
            if "*" in str(artefact):
                raise ValueError("Glob patterns not supported for Snowpark artifacts.")
        return artifacts

    @property
    def udf_sproc_identifier(self) -> UdfSprocIdentifier:
        return UdfSprocIdentifier.from_definition(self)


class ProcedureEntityModel(SnowparkEntityModel):
    type: Literal["procedure"] = DiscriminatorField()  # noqa: A003
    execute_as_caller: Optional[bool] = Field(
        title="Determine whether the procedure is executed with the privileges of "
        "the owner (you) or with the privileges of the caller",
        default=False,
    )


class FunctionEntityModel(SnowparkEntityModel):
    type: Literal["function"] = DiscriminatorField()  # noqa: A003


class UdfSprocIdentifier:
    def __init__(self, identifier: FQN, arg_names, arg_types, arg_defaults):
        self._identifier = identifier
        self._arg_names = arg_names
        self._arg_types = arg_types
        self._arg_defaults = arg_defaults

    def _identifier_from_signature(self, sig: List[str], for_sql: bool = False):
        signature = self._comma_join(sig)
        id_ = self._identifier.sql_identifier if for_sql else self._identifier
        return f"{id_}({signature})"

    @staticmethod
    def _comma_join(*args):
        return ", ".join(*args)

    @property
    def identifier_with_arg_names(self):
        return self._identifier_from_signature(self._arg_names)

    @property
    def identifier_with_arg_types(self):
        return self._identifier_from_signature(self._arg_types)

    @property
    def identifier_with_arg_names_types(self):
        sig = [f"{n} {t}" for n, t in zip(self._arg_names, self._arg_types)]
        return self._identifier_from_signature(sig)

    @property
    def identifier_with_arg_names_types_defaults(self):
        return self._identifier_from_signature(self._full_signature())

    def _is_signature_type_a_string(self, sig_type: str) -> bool:
        return sig_type.lower() in ["string", "varchar"]

    def _full_signature(self):
        sig = []
        for name, _type, _default in zip(
            self._arg_names, self._arg_types, self._arg_defaults
        ):
            s = f"{name} {_type}"
            if _default:
                if self._is_signature_type_a_string(_type):
                    _default = f"'{_default}'"
                s += f" default {_default}"
            sig.append(s)
        return sig

    @property
    def identifier_for_sql(self):
        return self._identifier_from_signature(self._full_signature(), for_sql=True)

    @classmethod
    def from_definition(cls, udf_sproc: SnowparkEntityModel):
        names = []
        types = []
        defaults = []
        if udf_sproc.signature and udf_sproc.signature != "null":
            for arg in udf_sproc.signature:
                names.append(arg.name)  # type:ignore
                types.append(arg.arg_type)  # type:ignore
                defaults.append(arg.default)  # type:ignore

        identifier = udf_sproc.fqn.using_context()
        return cls(identifier, names, types, defaults)
