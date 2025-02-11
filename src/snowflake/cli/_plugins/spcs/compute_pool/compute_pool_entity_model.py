from typing import List, Literal, Optional

from pydantic import Field
from snowflake.cli._plugins.object.common import Tag
from snowflake.cli.api.project.schemas.entities.common import EntityModelBase
from snowflake.cli.api.project.schemas.updatable_model import DiscriminatorField


class ComputePoolEntityModel(EntityModelBase):
    type: Literal["compute-pool"] = DiscriminatorField()  # noqa: A003
    min_nodes: int = Field(title="Minimum number of nodes", default=None, ge=1)
    max_nodes: Optional[int] = Field(
        title="Maximum number of nodes", default=None, ge=1
    )
    instance_family: str = Field(title="Name of the instance family", default=None)
    auto_resume: Optional[bool] = Field(
        title="The compute pool will automatically resume when a service or job is submitted to it",
        default=True,
    )
    initially_suspended: Optional[bool] = Field(
        title="Starts the compute pool in a suspended state", default=False
    )
    auto_suspend_seconds: Optional[int] = Field(
        title="Number of seconds of inactivity after which you want Snowflake to automatically suspend the compute pool",
        default=3600,
        ge=1,
    )
    comment: Optional[str] = Field(title="Comment for the compute pool", default=None)
    tags: Optional[List[Tag]] = Field(title="Tag for the service", default=None)
