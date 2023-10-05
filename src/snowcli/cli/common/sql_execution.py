from __future__ import annotations
from contextlib import contextmanager

from textwrap import dedent
from functools import cached_property

from snowflake.connector.errors import ProgrammingError
from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager
from snowflake.connector.cursor import DictCursor


class SqlExecutionMixin:
    def __init__(self):
        pass

    @cached_property
    def _definition(self):
        # For use in Manager.py, you should be able to have full access to snowflake.yml, not limited to your own product
        return snow_cli_global_context_manager.get_definition_manager

    @cached_property
    def _conn(self):
        return snow_cli_global_context_manager.get_connection()

    def _execute_query(self, query: str, **kwargs):
        *_, last_result = self._conn.execute_string(dedent(query), **kwargs)
        return last_result

    def _execute_queries(self, queries: str, **kwargs):
        return self._conn.execute_string(dedent(queries), **kwargs)

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
            self._execute_query(f"use role {new_role}")
        try:
            yield
        finally:
            if is_different_role:
                self._execute_query(f"use role {prev_role}")

    def _execute_schema_query(self, query: str):
        self.check_database_and_schema()
        return self._execute_query(query)

    def check_database_and_schema(self) -> None:
        database = self._conn.database
        schema = self._conn.schema
        self.check_database_exists(database)
        self.check_schema_exists(database, schema)

    def check_database_exists(self, database: str) -> None:
        if not database:
            raise Exception(
                """
                Database not specified. Please update connection to add `DATABASE` parameter,
                or re-run command using `--dbname` option.
                Use `snow connection list` to list existing connections
                """
            )
        try:
            self._execute_query(f"USE DATABASE {database}")
        except ProgrammingError as e:
            raise Exception(
                f"""
            Exception occurred: {e}. Make sure you have `DATABASE` parameter in connection or `--dbname` option provided
            Use `snow connection list` to list existing connections
            """
            ) from e

    def check_schema_exists(self, database: str, schema: str) -> None:
        if not schema:
            raise Exception(
                """
                Schema not specified. Please update connection to add `SCHEMA` parameter,
                or re-run command using `--schema` option.
                Use `snow connection list` to list existing connections
                """
            )
        try:
            self._execute_query(f"USE {database}.{schema}")
        except ProgrammingError as e:
            raise Exception(
                f"""
            Exception occurred: {e}. Make sure you have `SCHEMA` parameter in connection or `--schema` option provided
            Use `snow connection list` to list existing connections
            """
            ) from e
