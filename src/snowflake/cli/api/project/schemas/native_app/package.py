from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import Field, field_validator
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
    UpdatableModel,
)

DistributionOptions = Literal["internal", "external", "INTERNAL", "EXTERNAL"]


class Package(UpdatableModel):
    scripts: Optional[List[str]] = Field(
        title="List of SQL file paths relative to the project root", default=None
    )
    role: Optional[str] = IdentifierField(
        title="Role to use when creating the application package and provider-side objects",
        default=None,
    )
    name: Optional[str] = IdentifierField(
        title="Name of the application package created when you run the snow app run command",
        default=None,
    )
    warehouse: Optional[str] = IdentifierField(
        title="Warehouse used to run the scripts", default=None
    )
    distribution: Optional[DistributionOptions] = Field(
        title="Distribution of the application package created by the Snowflake CLI",
        default="internal",
    )

    @field_validator("scripts")
    @classmethod
    def validate_scripts(cls, input_list):
        if len(input_list) != len(set(input_list)):
            raise ValueError(
                "package.scripts field should contain unique values. Check the list for duplicates and try again"
            )
        return input_list
