from __future__ import annotations

from pydantic import Field
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.snowpark.snowpark import Snowpark
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class ProjectDefinition(UpdatableModel):
    definition_version: int = Field(
        title="Version of the project definition schema, which is currently 1",
        ge=1,
        le=1,
    )
    native_app: NativeApp | None = Field(
        title="Native app definitions for the project", default=None
    )
    snowpark: Snowpark | None = Field(
        title="Snowpark functions and procedures definitions for the project",
        default=None,
    )
    streamlit: Streamlit | None = Field(
        title="Streamlit definitions for the project", default=None
    )
