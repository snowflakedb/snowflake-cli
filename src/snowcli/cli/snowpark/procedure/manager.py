from __future__ import annotations

from typing import List

from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.snowpark.common import SnowparkObjectManager


class ProcedureManager(SnowparkObjectManager):
    @property
    def _object_type(self):
        return "procedure"

    def execute(self, expression: str) -> SnowflakeCursor:
        return self._execute_query(f"call {expression}")

    def create(
        self,
        identifier: str,
        return_type: str,
        handler: str,
        artifact_file: str,
        packages: List[str],
        overwrite: bool,
        execute_as_caller: bool,
    ) -> SnowflakeCursor:
        create_stmt = "create or replace" if overwrite else "create"
        packages_list = ",".join(f"'{p}'" for p in packages)
        query = f"""\
            {create_stmt} procedure {identifier}
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
