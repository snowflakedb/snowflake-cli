from typing import Literal

from pydantic import Field
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.project.schemas.entities.common import EntityModelBase
from snowflake.cli.api.project.schemas.updatable_model import DiscriminatorField


class ServiceEntityModel(EntityModelBase):
    type: Literal["snowpark container service"] = DiscriminatorField()  # noqa: A003
    specification_file: str = Field(
        title="Path to the specification file for the SPCS service, relative to the deploy root",
    )
    # TODO is a compute pool a separate entity?
    compute_pool: str = Field(
        title="Name of the compute pool to use for the SPCS service",
    )
    min_nodes: int = Field(
        title="Minimum number of nodes in the compute pool",
        default=1,
    )
    max_nodes: int = Field(
        title="Maximum number of nodes in the compute pool",
        default=1,
    )
    instance_family: str = Field(
        title="Instance family to use for the compute pool",
        default="CPU_X64_XS",
    )


class ServiceEntity(EntityBase[ServiceEntityModel]):
    # Local deploy of SPSC service not yet implemented
    # We only use the model to deploy SPCS services in native apps
    pass
