from __future__ import annotations

from typing import List

from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.snowpark.common import SnowparkObjectManager


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
        packages_list = ",".join(f"'{p}'" for p in packages)
        return self._execute_query(
            f"""\
            create or replace function {identifier}
            returns {return_type}
            language python
            runtime_version=3.8
            imports=('@{artifact_file}')
            handler='{handler}'
            packages=({packages_list})
        """
        )
