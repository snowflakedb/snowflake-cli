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
    bundle_root: Optional[str] = Field(
        title="Folder at the root of your project where artifacts necessary to perform the bundle step are stored.",
        default="output/bundle/",
    )
    deploy_root: Optional[str] = Field(
        title="Folder at the root of your project where the build step copies the artifacts.",
        default="output/deploy/",
    )
    generated_root: Optional[str] = Field(
        title="Subdirectory of the deploy root where files generated by the Snowflake CLI will be written.",
        default="__generated/",
    )
    source_stage: Optional[str] = Field(
        title="Identifier of the stage that stores the application artifacts.",
        default="app_src.stage",
    )
    scratch_stage: Optional[str] = Field(
        title="Identifier of the stage that stores temporary scratch data used by the Snowflake CLI.",
        default="app_src.stage_snowflake_cli_scratch",
    )
    package: Optional[Package] = Field(title="PackageSchema", default=None)
    application: Optional[Application] = Field(title="Application info", default=None)

    @field_validator("source_stage")
    @classmethod
    def validate_source_stage(cls, input_value: str):
        if not re.match(SCHEMA_AND_NAME, input_value):
            raise ValueError("Incorrect value for source_stage value of native_app")
        return input_value

    @field_validator("artifacts")
    @classmethod
    def transform_artifacts(
        cls, orig_artifacts: List[Union[PathMapping, str]]
    ) -> List[PathMapping]:
        transformed_artifacts = []
        if orig_artifacts is None:
            return transformed_artifacts

        for artifact in orig_artifacts:
            if isinstance(artifact, PathMapping):
                transformed_artifacts.append(artifact)
            else:
                transformed_artifacts.append(PathMapping(src=artifact))

        return transformed_artifacts
