from __future__ import annotations

from typing import List

from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.snowpark.common import SnowparkObjectManager


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
        execute_as_caller: bool,
    ) -> SnowflakeCursor:
        packages_list = ",".join(f"'{p}'" for p in packages)
        query = f"""\
            create or replace procedure {identifier}
            returns {return_type}
            language python
            runtime_version=3.8
            imports=('@{artifact_file}')
            handler='{handler}'
            packages=({packages_list})
        """
        if execute_as_caller:
            query += "\nexecute as caller"
        return self._execute_query(query)
