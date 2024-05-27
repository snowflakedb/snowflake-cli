from __future__ import annotations

from typing import Any, Dict, Optional, Union

from packaging.version import Version
from pydantic import Field, field_validator
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.snowpark.snowpark import Snowpark
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel
from snowflake.cli.api.utils.models import EnvironWithDefinedDictFallback


class _BaseDefinition(UpdatableModel):
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
    env: Optional[Dict] = Field(
        title="Environment specification for this project.",
        default=None,
        validation_alias="env",
    )

    @field_validator("env")
    @classmethod
    def _convert_env(cls, env: Optional[Dict]) -> EnvironWithDefinedDictFallback:
        variables = EnvironWithDefinedDictFallback(env if env else {})
        return variables


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
