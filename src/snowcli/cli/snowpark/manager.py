from __future__ import annotations

import logging
from typing import Dict, List, Optional

from snowcli.cli.snowpark.common import SnowparkObjectManager
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__file__)


class FunctionManager(SnowparkObjectManager):
    @property
    def _object_type(self):
        return "function"

    @property
    def _object_execute(self):
        return "select"

    def create_or_replace(
        self,
        identifier: str,
        return_type: str,
        handler: str,
        artifact_file: str,
        packages: List[str],
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
    ) -> SnowflakeCursor:
        log.debug(f"Creating function {identifier} using @{artifact_file}")
        query = self.create_query(
            identifier,
            return_type,
            handler,
            artifact_file,
            packages,
            external_access_integrations,
            secrets,
        )
        return self._execute_query(query)


class ProcedureManager(SnowparkObjectManager):
    @property
    def _object_type(self):
        return "procedure"

    @property
    def _object_execute(self):
        return "call"

    def create_or_replace(
        self,
        identifier: str,
        return_type: str,
        handler: str,
        artifact_file: str,
        packages: List[str],
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        execute_as_caller: bool = False,
    ) -> SnowflakeCursor:
        log.debug(f"Creating procedure {identifier} using @{artifact_file}")
        query = self.create_query(
            identifier,
            return_type,
            handler,
            artifact_file,
            packages,
            external_access_integrations,
            secrets,
            execute_as_caller,
        )
        return self._execute_query(query)
