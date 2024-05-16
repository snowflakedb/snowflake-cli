from __future__ import annotations

from typing import Dict, Optional, Union

from packaging.version import Version
from pydantic import Field, field_validator
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.snowpark.snowpark import Snowpark
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel
from snowflake.cli.api.utils.models import DictWithEnvironFallback

# todo: update examples
_supported_version = ("1", "1.1")
_latest_version = "1.1"


class ProjectDefinition(UpdatableModel):
    definition_version: Union[str, int] = Field(
        title="Version of the project definition schema, which is currently 1",
    )
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
    env: Optional[Dict] = Field(
        title="Environment specification for this project.",
        default=None,
        validation_alias="env",
    )

    @field_validator("env")
    @classmethod
    def _convert_env(cls, env: Optional[Dict]) -> DictWithEnvironFallback:
        variables = DictWithEnvironFallback(env if env else {})
        for key in variables:
            # Accessing value first checks for env var, so in this way we update the in-memory state
            variables[key] = variables[key]
        return variables

    @field_validator("definition_version")
    @classmethod
    def _is_supported_version(cls, version: str) -> str:
        version = str(version)
        if version not in _supported_version:
            raise ValueError(
                f'Version {version} is not supported. Supported versions: {", ".join(_supported_version)}'
            )
        return version

    def meets_version_requirement(self, required_version: str) -> bool:
        return Version(self.definition_version) >= Version(required_version)
