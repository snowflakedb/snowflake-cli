from __future__ import annotations

from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class ObjectManager(SqlExecutionMixin):
    def show(self, object_type: str, like: str) -> SnowflakeCursor:
        return self._execute_query(f"show {object_type} like '{like}'")

    def drop(self, object_type, name: str) -> SnowflakeCursor:
        return self._execute_query(f"drop {object_type} {name}")

    def describe(self, object_type: str, name: str):
        return self._execute_query(f"describe {object_type} {name}")
