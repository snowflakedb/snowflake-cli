from __future__ import annotations

from textwrap import dedent

from click import ClickException
from snowflake.connector.errors import ProgrammingError
from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager


class SqlExecutionMixin:
    def __init__(self):
        pass

    @property
    def _conn(self):
        return snow_cli_global_context_manager.get_connection()

    def _execute_query(self, query: str):
        *_, last_result = self._conn.execute_string(dedent(query))
        return last_result

    def _execute_queries(self, queries: str):
        return self._conn.execute_string(dedent(queries))

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

    def to_fully_qualified_name(self, name: str):
        current_parts = name.split(".")
        if len(current_parts) == 3:
            # already fully qualified name
            return name.upper()

        if not self._conn.database:
            raise ClickException(
                "Default database not specified in connection details."
            )

        if len(current_parts) == 2:
            # we assume this is schema.object
            return f"{self._conn.database}.{name}".upper()

        schema = self._conn.schema or "public"
        return f"{self._conn.database}.{schema}.{name}".upper()
