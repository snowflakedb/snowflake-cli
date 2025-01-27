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

from typing import Literal, Optional

from pydantic import Field
from snowflake.cli.api.project.schemas.entities.common import (
    Artifacts,
    EntityModelBaseWithArtifacts,
    ExternalAccessBaseModel,
    GrantBaseModel,
    ImportsBaseModel,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
)


class StreamlitEntityModel(
    EntityModelBaseWithArtifacts,
    ExternalAccessBaseModel,
    ImportsBaseModel,
    GrantBaseModel,
):
    type: Literal["streamlit"] = DiscriminatorField()  # noqa: A003
    title: Optional[str] = Field(
        title="Human-readable title for the Streamlit dashboard", default=None
    )
    comment: Optional[str] = Field(title="Comment for the Streamlit app", default=None)
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
    # Artifacts were optional, so to avoid BCR, we need to make them optional here as well
    artifacts: Optional[Artifacts] = Field(
        title="List of paths or file source/destination pairs to add to the deploy root",
        default=None,
    )
