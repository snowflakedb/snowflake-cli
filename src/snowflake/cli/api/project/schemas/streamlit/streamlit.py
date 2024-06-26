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
from typing import List, Optional

from pydantic import Field
from snowflake.cli.api.project.schemas.identifier_model import ObjectIdentifierModel
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class Streamlit(UpdatableModel, ObjectIdentifierModel(object_name="Streamlit")):  # type: ignore
    stage: Optional[str] = Field(
        title="Stage in which the app’s artifacts will be stored", default="streamlit"
    )
    query_warehouse: str = Field(
        title="Snowflake warehouse to host the app", default="streamlit"
    )
    main_file: Optional[Path] = Field(
        title="Entrypoint file of the Streamlit app", default="streamlit_app.py"
    )
    env_file: Optional[Path] = Field(
        title="File defining additional configurations for the app, such as external dependencies",
        default=None,
    )
    pages_dir: Optional[Path] = Field(title="Streamlit pages", default=None)
    additional_source_files: Optional[List[Path]] = Field(
        title="List of additional files which should be included into deployment artifacts",
        default=None,
    )
    title: Optional[str] = Field(
        title="Human-readable title for the Streamlit dashboard", default=None
    )
