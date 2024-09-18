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
from typing import List, Literal, Optional

from pydantic import Field, model_validator
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBase,
    ExternalAccessBaseModel,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
)


class StreamlitEntityModel(EntityModelBase, ExternalAccessBaseModel):
    type: Literal["streamlit"] = DiscriminatorField()  # noqa: A003
    title: Optional[str] = Field(
        title="Human-readable title for the Streamlit dashboard", default=None
    )
    query_warehouse: str = Field(
        title="Snowflake warehouse to host the app", default=None
    )
    main_file: Optional[str] = Field(
        title="Entrypoint file of the Streamlit app", default="streamlit_app.py"
    )
    pages_dir: Optional[str] = Field(title="Streamlit pages", default=None)
    stage: Optional[str] = Field(
        title="Stage in which the app’s artifacts will be stored", default="streamlit"
    )
    # Possibly can be PathMapping
    artifacts: Optional[List[Path]] = Field(
        title="List of files which should be deployed. Each file needs to exist locally. "
        "Main file needs to be included in the artifacts.",
        default=None,
    )

    @model_validator(mode="after")
    def main_file_must_be_in_artifacts(self):
        if not self.artifacts:
            return self

        if Path(self.main_file) not in self.artifacts:
            raise ValueError(
                f"Specified main file {self.main_file} is not included in artifacts."
            )
        return self

    @model_validator(mode="after")
    def artifacts_must_exists(self):
        if not self.artifacts:
            return self

        for artifact in self.artifacts:
            if not artifact.exists():
                raise ValueError(
                    f"Specified artifact {artifact} does not exist locally."
                )

        return self
