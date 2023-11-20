from __future__ import annotations

from functools import wraps

from click import ClickException
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.constants import OBJECT_TO_NAMES, ObjectNames
from snowflake.connector.cursor import SnowflakeCursor


def _get_object_names(object_name: str) -> ObjectNames:
    object_name = object_name.lower()
    if object_name.lower() not in OBJECT_TO_NAMES:
        raise ClickException(f"Object of type {object_name} is not supported.")
    return OBJECT_TO_NAMES[object_name]


class ObjectManager(SqlExecutionMixin):
    def show(self, *, object_type: str, like: str) -> SnowflakeCursor:
        object_name = _get_object_names(object_type).sf_plural_name
        return self._execute_query(f"show {object_name} like '{like}'")

    def drop(self, *, object_type, name: str) -> SnowflakeCursor:
        object_name = _get_object_names(object_type).sf_name
        return self._execute_query(f"drop {object_name} {name}")

    def describe(self, *, object_type: str, name: str):
        object_name = _get_object_names(object_type).sf_name
        return self._execute_query(f"describe {object_name} {name}")
