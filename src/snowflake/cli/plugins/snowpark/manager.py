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

import logging
from typing import Dict, List, Optional

from snowflake.cli.api.constants import ObjectType
from snowflake.cli.plugins.snowpark.common import (
    SnowparkObjectManager,
    UdfSprocIdentifier,
)
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)


class FunctionManager(SnowparkObjectManager):
    @property
    def _object_type(self):
        return ObjectType.FUNCTION

    @property
    def _object_execute(self):
        return "select"

    def create_or_replace(
        self,
        identifier: UdfSprocIdentifier,
        return_type: str,
        handler: str,
        artifact_file: str,
        packages: List[str],
        imports: List[str],
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        runtime: Optional[str] = None,
    ) -> SnowflakeCursor:
        log.debug(
            "Creating function %s using @%s",
            identifier.identifier_with_arg_names_types_defaults,
            artifact_file,
        )
        query = self.create_query(
            identifier,
            return_type,
            handler,
            artifact_file,
            packages,
            imports,
            external_access_integrations,
            secrets,
            runtime,
        )
        return self._execute_query(query)


class ProcedureManager(SnowparkObjectManager):
    @property
    def _object_type(self):
        return ObjectType.PROCEDURE

    @property
    def _object_execute(self):
        return "call"

    def create_or_replace(
        self,
        identifier: UdfSprocIdentifier,
        return_type: str,
        handler: str,
        artifact_file: str,
        packages: List[str],
        imports: List[str],
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        runtime: Optional[str] = None,
        execute_as_caller: bool = False,
    ) -> SnowflakeCursor:
        log.debug(
            "Creating procedure %s using @%s",
            identifier.identifier_with_arg_names_types_defaults,
            artifact_file,
        )
        query = self.create_query(
            identifier,
            return_type,
            handler,
            artifact_file,
            packages,
            imports,
            external_access_integrations,
            secrets,
            runtime,
            execute_as_caller,
        )
        return self._execute_query(query)
