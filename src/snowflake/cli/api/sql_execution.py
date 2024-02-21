from __future__ import annotations

import logging
from contextlib import contextmanager
from functools import cached_property
from io import StringIO
from textwrap import dedent
from typing import Iterable, Optional

from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.exceptions import (
    DatabaseNotProvidedError,
    SchemaNotProvidedError,
    SnowflakeSQLExecutionError,
)
from snowflake.cli.api.project.util import (
    identifier_to_show_like_pattern,
    unquote_identifier,
)
from snowflake.cli.api.utils.cursor import find_first_row
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from snowflake.connector.errors import ProgrammingError


class SqlExecutionMixin:
    def __init__(self):
        pass

    @property
    def _conn(self):
        return cli_context.connection

    @cached_property
    def _log(self):
        return logging.getLogger(__name__)

    def _execute_string(
        self,
        sql_text: str,
        remove_comments: bool = False,
        return_cursors: bool = True,
        cursor_class: SnowflakeCursor = SnowflakeCursor,
        **kwargs,
    ) -> Iterable[SnowflakeCursor]:
        """
        This is a custom implementation of SnowflakeConnection.execute_string that returns generator
        instead of list. In case of executing multiple queries are executed one by one. This mean we can
        access result of previous queries while evaluating next one. For example, we can print the results.
        """
        self._log.debug("Executing %s", sql_text)
        stream = StringIO(sql_text)
        stream_generator = self._conn.execute_stream(
            stream, remove_comments=remove_comments, cursor_class=cursor_class, **kwargs
        )
        return stream_generator if return_cursors else list()

    def _execute_query(self, query: str, **kwargs):
        *_, last_result = self._execute_queries(query, **kwargs)
        return last_result

    def _execute_queries(self, queries: str, **kwargs):
        return list(self._execute_string(dedent(queries), **kwargs))

    @contextmanager
    def use_role(self, new_role: str):
        """
        Switches to a different role for a while, then switches back.
        This is a no-op if the requested role is already active.
        """
        role_result = self._execute_query(
            f"select current_role()", cursor_class=DictCursor
        ).fetchone()
        prev_role = role_result["CURRENT_ROLE()"]
        is_different_role = new_role.lower() != prev_role.lower()
        if is_different_role:
            self._log.debug("Assuming different role: %s", new_role)
            self._execute_query(f"use role {new_role}")
        try:
            yield
        finally:
            if is_different_role:
                self._execute_query(f"use role {prev_role}")

    def _execute_schema_query(self, query: str, **kwargs):
        self.check_database_and_schema()
        return self._execute_query(query, **kwargs)

    def check_database_and_schema(self) -> None:
        """
        Checks if the connection database and schema are set and that they actually exist in Snowflake.
        """
        self.check_schema_exists(self._conn.database, self._conn.schema)

    def check_database_exists(self, database: str) -> None:
        """
        Checks that database is provided and that it is a valid database in
        Snowflake. Note that this could fail for a variety of reasons,
        including not authorized to use database, database doesn't exist,
        database is not a valid identifier, and more.
        """
        if not database:
            raise DatabaseNotProvidedError()
        try:
            self._execute_query(f"USE DATABASE {database}")
        except ProgrammingError as e:
            raise ClickException(f"Exception occurred: {e}.") from e

    def check_schema_exists(self, database: str, schema: str) -> None:
        """
        Checks that schema is provided and that it is a valid schema in Snowflake.
        Note that this could fail for a variety of reasons,
        including not authorized to use schema, schema doesn't exist,
        schema is not a valid identifier, and more.
        """
        self.check_database_exists(database)
        if not schema:
            raise SchemaNotProvidedError()
        try:
            self._execute_query(f"USE {database}.{schema}")
        except ProgrammingError as e:
            raise ClickException(f"Exception occurred: {e}.") from e

    def to_fully_qualified_name(
        self, name: str, database: Optional[str] = None, schema: Optional[str] = None
    ):
        current_parts = name.split(".")
        if len(current_parts) == 3:
            # already fully qualified name
            return name.upper()

        if not database:
            if not self._conn.database:
                raise ClickException(
                    "Default database not specified in connection details."
                )
            database = self._conn.database

        if len(current_parts) == 2:
            # we assume name is in form of `schema.object`
            return f"{database}.{name}".upper()

        schema = schema or self._conn.schema or "public"
        database = database or self._conn.database
        return f"{database}.{schema}.{name}".upper()

    def show_specific_object(
        self,
        object_type_plural: str,
        unqualified_name: str,
        name_col: str = "name",
        in_clause: str = "",
        check_schema: bool = False,
    ) -> Optional[dict]:
        """
        Executes a "show <objects> like" query for a particular entity with a
        given (unqualified) name. This command is useful when the corresponding
        "describe <object>" query does not provide the information you seek.
        """
        if check_schema:
            self.check_database_and_schema()
        show_obj_query = f"show {object_type_plural} like {identifier_to_show_like_pattern(unqualified_name)} {in_clause}".strip()
        show_obj_cursor = self._execute_query(  # type: ignore
            show_obj_query, cursor_class=DictCursor
        )
        if show_obj_cursor.rowcount is None:
            raise SnowflakeSQLExecutionError(show_obj_query)
        show_obj_row = find_first_row(
            show_obj_cursor,
            lambda row: row[name_col] == unquote_identifier(unqualified_name),
        )
        return show_obj_row
