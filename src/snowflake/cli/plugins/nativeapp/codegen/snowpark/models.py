from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import Field
from snowflake.cli.api.project.schemas.snowpark.callable import _CallableBase
from snowflake.cli.api.project.schemas.updatable_model import IdentifierField


class ExtensionFunctionTypeEnum(str, Enum):
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    TABLE_FUNCTION = "TABLE_FUNCTION"
    AGGREGATE_FUNCTION = "AGGREGATE_FUNCTION"


class NativeAppExtensionFunction(_CallableBase):
    function_type: ExtensionFunctionTypeEnum
    lineno: Optional[int] = Field(
        title="The line number of the extension function", default=None
    )
    name: Optional[str] = Field(title="The name of the external function", default=None)
    packages: Optional[List[str]] = Field(
        title="List of packages (with optional version constraints) to be loaded for the function",
        default={},
    )
    schema_name: Optional[str] = IdentifierField(
        title=f"Name of the schema for the function",
        default=None,
        alias="schema",
    )
    application_roles: Optional[List[str]] = Field(
        title="Application roles granted usage to the function",
        default=[],
    )
    execute_as_caller: Optional[bool] = Field(
        title="Determine whether the procedure is executed with the privileges of "
        "the owner or with the privileges of the caller",
        default=None,
    )
