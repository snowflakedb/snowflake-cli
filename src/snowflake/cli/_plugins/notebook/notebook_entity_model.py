from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

from pydantic import Field, model_validator
from snowflake.cli._plugins.notebook.exceptions import NotebookFilePathError
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBaseWithArtifacts,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
)


class NotebookEntityModel(EntityModelBaseWithArtifacts):
    type: Literal["notebook"] = DiscriminatorField()  # noqa: A003
    stage_path: Optional[str] = Field(
        title="Stage directory in which the notebook file will be stored", default=None
    )
    notebook_file: Path = Field(title="Notebook file")
    query_warehouse: str = Field(title="Snowflake warehouse to execute the notebook")

    @model_validator(mode="after")
    def validate_notebook_file(self):
        if not self.notebook_file.exists():
            raise ValueError(f"Notebook file {self.notebook_file} does not exist")
        if self.notebook_file.suffix.lower() != ".ipynb":
            raise NotebookFilePathError(str(self.notebook_file))
        return self
