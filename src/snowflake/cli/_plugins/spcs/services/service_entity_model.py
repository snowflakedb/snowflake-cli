from pathlib import Path
from typing import List, Literal, Optional

from pydantic import Field, field_validator
from snowflake.cli._plugins.object.common import Tag
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBaseWithArtifacts,
    ExternalAccessBaseModel,
)
from snowflake.cli.api.project.schemas.updatable_model import DiscriminatorField
from snowflake.cli.api.project.util import to_string_literal


class ServiceEntityModel(EntityModelBaseWithArtifacts, ExternalAccessBaseModel):
    type: Literal["service"] = DiscriminatorField()  # noqa: A003
    stage: str = Field(
        title="Stage where the service specification file is located", default=None
    )
    compute_pool: str = Field(title="Compute pool to run the service on", default=None)
    spec_file: Path = Field(
        title="Path to service specification file on stage", default=None
    )
    min_instances: Optional[int] = Field(
        title="Minimum number of instances", default=1, ge=1
    )
    max_instances: Optional[int] = Field(
        title="Maximum number of instances", default=None, ge=1
    )
    auto_resume: bool = Field(
        title="The service will automatically resume when a service function or ingress is called.",
        default=True,
    )
    query_warehouse: Optional[str] = Field(
        title="Warehouse to use if a service container connects to Snowflake to execute a query without explicitly specifying a warehouse to use",
        default=None,
    )
    tags: Optional[List[Tag]] = Field(title="Tag for the service", default=None)
    comment: Optional[str] = Field(title="Comment for the service", default=None)

    @field_validator("comment")
    @classmethod
    def _convert_artifacts(cls, comment: Optional[str]):
        if comment:
            return to_string_literal(comment)
        return comment
