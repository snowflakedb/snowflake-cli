from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

from pydantic import Field, model_validator
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBase,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
)


class NotebookEntityModel(EntityModelBase):
    type: Literal["notebook"] = DiscriminatorField()  # noqa: A003
    stage: Optional[str] = Field(
        title="Stage in which the notebook file will be stored", default="notebooks"
    )
    notebook_file: Path = Field(title="Notebook file")

    @model_validator(mode="after")
    def notebook_file_must_exist(self):
        if not self.notebook_file.exists():
            raise ValueError(f"Notebook file {self.notebook_file} does not exist")
        return self
