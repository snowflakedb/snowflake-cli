from __future__ import annotations

from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.object.utils import ObjectType


class ObjectManager(SqlExecutionMixin):
    def show(self, object_type: ObjectType, like: str) -> SnowflakeCursor:
        return self._execute_query(f"show {object_type.plural} like '{like}'")

    def drop(self, object_type: ObjectType, name: str) -> SnowflakeCursor:
        return self._execute_query(f"drop {object_type.singular} '{name}'")

    def describe(self, object_type: ObjectType):
        pass
