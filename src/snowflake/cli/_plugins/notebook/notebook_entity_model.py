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
    compute_pool: Optional[str] = Field(
        title="Compute pool to run the notebook in", default=None
    )
    runtime_name: Optional[str] = Field(title="Container Runtime for ML", default=None)

    @model_validator(mode="after")
    def validate_notebook_file(self):
        if not self.notebook_file.exists():
            raise ValueError(f"Notebook file {self.notebook_file} does not exist")
        if self.notebook_file.suffix.lower() != ".ipynb":
            raise NotebookFilePathError(str(self.notebook_file))
        return self

    @model_validator(mode="after")
    def validate_container_setup(self):
        if self.compute_pool and not self.runtime_name:
            raise ValueError("compute_pool is specified without runtime_name")
        if self.runtime_name and not self.compute_pool and not self:
            raise ValueError("runtime_name is specified without compute_pool")
        return self
