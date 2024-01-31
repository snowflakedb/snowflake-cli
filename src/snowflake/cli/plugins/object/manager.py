from __future__ import annotations

from click import ClickException
from snowflake.cli.api.constants import OBJECT_TO_NAMES, ObjectNames
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


def _get_object_names(object_type: str) -> ObjectNames:
    object_type = object_type.lower()
    if object_type.lower() not in OBJECT_TO_NAMES:
        raise ClickException(f"Object of type {object_type} is not supported.")
    return OBJECT_TO_NAMES[object_type]


class ObjectManager(SqlExecutionMixin):
    def show(
        self, *, object_type: str, like: str | None = None, **kwargs
    ) -> SnowflakeCursor:
        object_name = _get_object_names(object_type).sf_plural_name
        query = f"show {object_name}"
        if like:
            query += f" like '{like}'"
        return self._execute_query(query, **kwargs)

    def drop(self, *, object_type, name: str) -> SnowflakeCursor:
        object_name = _get_object_names(object_type).sf_name
        return self._execute_query(f"drop {object_name} {name}")

    def describe(self, *, object_type: str, name: str):
        # Image repository is the only supported object that does not have a DESCRIBE command.
        if object_type == "image-repository":
            raise ClickException(
                f"Describe is currently not supported for object of type image-repository"
            )
        object_name = _get_object_names(object_type).sf_name
        return self._execute_query(f"describe {object_name} {name}")
