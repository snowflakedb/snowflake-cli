from __future__ import annotations

from typing import List, Optional

from pydantic import Field
from snowflake.cli.api.project.schemas.snowpark.callable import (
    FunctionSchema,
    ProcedureSchema,
)
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


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
