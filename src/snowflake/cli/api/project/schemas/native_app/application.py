from __future__ import annotations

from typing import Literal, Optional

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
    UpdatableModel,
)


class Application(UpdatableModel):
    role: Optional[str] = Field(
        title="Role to use when creating the application instance and consumer-side objects",
        default=None,
    )
    name: Optional[str] = Field(
        title="Name of the application created when you run the snow app run command",
        default=None,
    )
    warehouse: Optional[str] = IdentifierField(
        title="Name of the application created when you run the snow app run command",
        default=None,
    )
    debug: Optional[bool] = Field(
        title="Whether to enable debug mode when using a named stage to create an application",
        default=True,
    )


DistributionOptions = Literal["internal", "external", "INTERNAL", "EXTERNAL"]
