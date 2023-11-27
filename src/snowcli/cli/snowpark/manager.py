from __future__ import annotations

import logging
from typing import List

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
    ) -> SnowflakeCursor:
        log.debug(f"Creating function {identifier} using @{artifact_file}")
        packages_list = ",".join(f"'{p}'" for p in packages)
        return self._execute_query(
            f"""\
            create or replace function {identifier}
            returns {return_type}
            language python
            runtime_version=3.8
            imports=('{artifact_file}')
            handler='{handler}'
            packages=({packages_list})
        """
        )


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
        execute_as_caller: bool = False,
    ) -> SnowflakeCursor:
        log.debug(f"Creating procedure {identifier} using @{artifact_file}")
        packages_list = ",".join(f"'{p}'" for p in packages)
        query = f"""\
            create or replace procedure {identifier}
            returns {return_type}
            language python
            runtime_version=3.8
            imports=('{artifact_file}')
            handler='{handler}'
            packages=({packages_list})
        """
        if execute_as_caller:
            query += "\nexecute as caller"
        return self._execute_query(query)
