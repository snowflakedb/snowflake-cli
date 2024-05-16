from __future__ import annotations

from typing import Optional, cast

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import IdentifierField


class ObjectIdentifierBaseModel:
    """
    Type representing a base class defining object that can be identified by fully qualified name (db.schema.name).
    This is not a Pydantic model and the purpose of this class is to provide typing support to Pydantic models
    generated using a factory class ObjectIdentifierModel.
    """

    name: str
    database: Optional[str]
    schema_name: Optional[str]


def ObjectIdentifierModel(object_name: str) -> ObjectIdentifierBaseModel:  # noqa: N802
    """Generates ObjectIdentifierBaseModel but with object specific descriptions."""

    class _ObjectIdentifierModel(ObjectIdentifierBaseModel):
        name: str = Field(title=f"{object_name} name")
        database: Optional[str] = IdentifierField(
            title=f"Name of the database for the {object_name}", default=None
        )
        schema_name: Optional[str] = IdentifierField(
            title=f"Name of the schema for the {object_name}",
            default=None,
            alias="schema",
        )

    return cast(ObjectIdentifierBaseModel, _ObjectIdentifierModel)
