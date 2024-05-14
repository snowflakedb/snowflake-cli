from __future__ import annotations

from typing import Optional, List, Union

from click import ClickException
from pydantic import Field, field_validator
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.snowpark.snowpark import Snowpark
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


# todo: update examples
_supported_version = ("1", "1.1")
_latest_version = "1.1"


class ProjectDefinition(UpdatableModel):
    definition_version: str = Field(
        title="Version of the project definition schema, which is currently 1",
        default=_latest_version
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
    env: Optional[List[Variable]] = Field(title="Environment specification for this project.", default=None)

    @field_validator('definition_version')
    @classmethod
    def _is_supported_version(cls, version: str) -> str:
        if version not in _supported_version:
            raise ClickException(
                f'Version {version} is not supported. Supported versions: {", ".join(_supported_version)}'
            )
        return version


VariableType = Union[str, bool, int, float]


class Variable(UpdatableModel):
    name: str = Field(title="Name of variable.")
    value: VariableType = Field(title="Value of variable.")
