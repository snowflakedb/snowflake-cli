from __future__ import annotations

from typing import Optional, cast

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import IdentifierField


class ObjectIdentifierBaseModel:
    name: str
    database: Optional[str]
    schema_name: Optional[str]


def identifier_model(object_name: str) -> ObjectIdentifierBaseModel:
    """Generates ObjectIdentifierBaseModel but with object specific descriptions."""

    class _ObjectIdentifierModel(ObjectIdentifierBaseModel):
        name: str = Field(title=f"{object_name} name")
        database: Optional[str] = IdentifierField(
            title=f"Name of the database for the for {object_name}", default=None
        )
        schema_name: Optional[str] = IdentifierField(
            title=f"Name of the schema for the {object_name}",
            default=None,
            alias="schema",
        )

    return cast(ObjectIdentifierBaseModel, _ObjectIdentifierModel)
