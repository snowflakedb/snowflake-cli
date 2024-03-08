from __future__ import annotations

import re
from typing import List, Optional, Union

from pydantic import Field, field_validator
from snowflake.cli.api.project.schemas.native_app.application import Application
from snowflake.cli.api.project.schemas.native_app.package import Package
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel
from snowflake.cli.api.project.util import (
    SCHEMA_AND_NAME,
)


class NativeApp(UpdatableModel):
    name: str = Field(
        title="Project identifier",
    )
    artifacts: List[Union[PathMapping, str]] = Field(
        title="List of file source and destination pairs to add to the deploy root",
    )
    deploy_root: Optional[str] = Field(
        title="Folder at the root of your project where the build step copies the artifacts.",
        default="output/deploy/",
    )
    source_stage: Optional[str] = Field(
        title="Identifier of the stage that stores the application artifacts.",
        default="app_src.stage",
    )
    package: Optional[Package] = Field(title="PackageSchema", default=None)
    application: Optional[Application] = Field(title="Application info", default=None)

    @field_validator("source_stage")
    @classmethod
    def validate_source_stage(cls, input_value: str):
        if not re.match(SCHEMA_AND_NAME, input_value):
            raise ValueError("Incorrect value for source_stage value of native_app")
        return input_value
