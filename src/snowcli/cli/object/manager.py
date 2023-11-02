from __future__ import annotations

from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.object.utils import ObjectType
from snowcli.cli.object.utils import get_plural_name


class ObjectManager(SqlExecutionMixin):
    def show(self, object_type: ObjectType, like: str) -> SnowflakeCursor:
        return self._execute_query(f"show {get_plural_name(object_type)} like '{like}'")

    def drop(self, object_type: ObjectType, name: str) -> SnowflakeCursor:
        return self._execute_query(f"drop {object_type.value} {name}")

    def describe(self, object_type: ObjectType, name: str):
        return self._execute_query(f"describe {object_type.value} {name}")
