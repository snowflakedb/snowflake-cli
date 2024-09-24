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

from typing import List, Optional

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel
from snowflake.cli.api.project.schemas.v1.snowpark.callable import (
    FunctionSchema,
    ProcedureSchema,
)


class Snowpark(UpdatableModel):
    project_name: str = Field(title="Project identifier")
    stage_name: str = Field(title="Stage in which project’s artifacts will be stored")
    src: str = Field(title="Folder where your code should be located")
    functions: Optional[List[FunctionSchema]] = Field(
        title="List of functions defined in the project", default=[]
    )
    procedures: Optional[List[ProcedureSchema]] = Field(
        title="List of procedures defined in the project", default=[]
    )
