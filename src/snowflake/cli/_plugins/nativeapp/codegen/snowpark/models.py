# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import IdentifierField
from snowflake.cli.api.project.schemas.v1.snowpark.callable import _CallableBase


class ExtensionFunctionTypeEnum(str, Enum):
    PROCEDURE = "procedure"
    FUNCTION = "function"
    TABLE_FUNCTION = "table function"
    AGGREGATE_FUNCTION = "aggregate function"


class NativeAppExtensionFunction(_CallableBase):
    function_type: ExtensionFunctionTypeEnum = Field(
        title="The type of extension function, one of 'procedure', 'function', 'table function' or 'aggregate function'",
        alias="type",
    )
    lineno: Optional[int] = Field(
        title="The starting line number of the extension function (1-based)",
        default=None,
    )
    name: Optional[str] = Field(
        title="The name of the extension function", default=None
    )
    packages: Optional[List[str]] = Field(
        title="List of packages (with optional version constraints) to be loaded for the function",
        default=[],
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
        default=False,
    )
