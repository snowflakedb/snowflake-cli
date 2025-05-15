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

import glob
from typing import List, Literal, Optional, Union

from pydantic import Field, field_validator, model_validator
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBaseWithArtifacts,
    ExternalAccessBaseModel,
    ImportsBaseModel,
    PathMapping,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
)
from snowflake.cli.api.project.schemas.v1.snowpark.argument import Argument


class SnowparkEntityModel(
    EntityModelBaseWithArtifacts, ExternalAccessBaseModel, ImportsBaseModel
):
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
    stage: str = Field(title="Stage in which artifacts will be stored")

    artifact_repository: Optional[str] = Field(
        default=None, title="Artifact repository to be used"
    )
    artifact_repository_packages: Optional[List[str]] = Field(
        default=None, title="Packages to be installed from artifact repository"
    )

    resource_constraint: Optional[dict] = Field(
        default=None, title="Resource constraints for the function/procedure"
    )

    @field_validator("artifacts")
    @classmethod
    def _convert_artifacts(cls, artifacts: Union[dict, str]):
        _artifacts = []
        for artifact in artifacts:
            if (
                (isinstance(artifact, str) and glob.has_magic(artifact))
                or (isinstance(artifact, PathMapping) and glob.has_magic(artifact.src))
            ) and FeatureFlag.ENABLE_SNOWPARK_GLOB_SUPPORT.is_disabled():
                raise ValueError(
                    "If you want to use glob patterns in artifacts, you need to enable the Snowpark new build feature flag (enable_snowpark_glob_support=true)"
                )
            if isinstance(artifact, PathMapping):
                _artifacts.append(artifact)
            else:
                _artifacts.append(PathMapping(src=artifact))
        return _artifacts

    @field_validator("runtime")
    @classmethod
    def convert_runtime(cls, runtime_input: Union[str, float]) -> str:
        if isinstance(runtime_input, float):
            return str(runtime_input)
        return runtime_input

    @model_validator(mode="before")
    @classmethod
    def check_artifact_repository(cls, values: dict) -> dict:
        artifact_repository = values.get("artifact_repository")
        artifact_repository_packages = values.get("artifact_repository_packages")
        if artifact_repository_packages and not artifact_repository:
            raise ValueError(
                "You specified Artifact_repository_packages without setting Artifact_repository.",
            )
        return values

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
            if _default is not None:
                if (
                    self._is_signature_type_a_string(_type)
                    and _default.lower() != "null"
                ):
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
