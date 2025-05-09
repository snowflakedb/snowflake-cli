# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
from contextlib import contextmanager
from functools import cached_property
from io import StringIO
from textwrap import dedent
from typing import Iterable, Optional, Tuple

from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import (
    CouldNotUseObjectError,
    DatabaseNotProvidedError,
    SchemaNotProvidedError,
    ShowSpecificObjectMultipleRowsError,
    SnowflakeSQLExecutionError,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import (
    identifier_to_show_like_pattern,
    to_identifier,
    unquote_identifier,
)
from snowflake.cli.api.utils.cursor import find_first_row
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from snowflake.connector.errors import ProgrammingError


class BaseSqlExecutor:
    """
    Base class for executing SQL queries on a Snowflake connection.

    This class provides methods to execute single or multiple SQL queries and handle the connection context.
    It also includes logging capabilities.
    """

    def __init__(self, connection: SnowflakeConnection | None = None) -> None:
        self._connection = connection

    @property
    def _conn(self) -> SnowflakeConnection:
        """Returns the current Snowflake connection, either from the instance or the global CLI context."""
        if self._connection:
            return self._connection
        return get_cli_context().connection

    @cached_property
    def _log(self):
        """Returns a logger instance for logging debug information."""
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
            stream,
            remove_comments=remove_comments,
            cursor_class=cursor_class,
            **kwargs,
        )

        return stream_generator if return_cursors else list()

    def execute_string(self, query: str, **kwargs) -> Iterable[SnowflakeCursor]:
        """Executes a single SQL query and returns the results"""
        return self._execute_string(query, **kwargs)

    def execute_query(self, query: str, **kwargs) -> SnowflakeCursor:
        """Executes a single SQL query and returns the last result"""
        *_, last_result = list(self.execute_string(dedent(query), **kwargs))
        return last_result

    def execute_queries(self, queries: str, **kwargs):
        """Executes multiple SQL queries (passed as one string) and returns the results as a list"""

        # Without remove_comments=True, connectors might throw an error if there is a comment at the end of the file
        return list(
            self.execute_string(dedent(queries), remove_comments=True, **kwargs)
        )


class SqlExecutor(BaseSqlExecutor):
    """
    SqlExecutor extends BaseSqlExecutor and provides additional methods to manage roles, warehouses,
    and create specific objects like secrets and API integrations.
    """

    def __init__(self, connection: SnowflakeConnection | None = None):
        super().__init__(connection)
        self._snowpark_session = None

    def use(self, object_type: ObjectType, name: str):
        try:
            self.execute_query(f"use {object_type.value.sf_name} {name}")
        except ProgrammingError as err:
            # Rewrite the error to make the message more useful.
            raise CouldNotUseObjectError(object_type=object_type, name=name) from err

    def current_role(self) -> str:
        return to_identifier(self.execute_query(f"select current_role()").fetchone()[0])

    @contextmanager
    def use_role(self, new_role: str):
        """
        Switches to a different role for a while, then switches back.
        This is a no-op if the requested role is already active.
        """
        new_role = to_identifier(new_role)
        prev_role = self.current_role()
        is_different_role = new_role.lower() != prev_role.lower()
        if is_different_role:
            self._log.debug("Assuming different role: %s", new_role)
            self.execute_query(f"use role {new_role}")
        try:
            yield
        finally:
            if is_different_role:
                self.execute_query(f"use role {prev_role}")

    def session_has_warehouse(self) -> bool:
        result = self.execute_query("select current_warehouse() is not null").fetchone()
        return bool(result[0])

    @contextmanager
    def use_warehouse(self, new_wh: str):
        """
        Switches to a different warehouse for a while, then switches back.
        This is a no-op if the requested warehouse is already active.
        If there is no default warehouse in the account, it will throw an error.
        """

        new_wh = to_identifier(new_wh)
        wh_result = self.execute_query(f"select current_warehouse()").fetchone()
        # If user has an assigned default warehouse, prev_wh will contain a value even if the warehouse is suspended.
        try:
            prev_wh = to_identifier(wh_result[0])
        except:
            prev_wh = None

        # new_wh is not None, and should already be a valid identifier, no additional check is performed here.
        is_different_wh = new_wh != prev_wh
        try:
            if is_different_wh:
                self._log.debug("Using warehouse: %s", new_wh)
                self.use(object_type=ObjectType.WAREHOUSE, name=new_wh)
            yield
        finally:
            if prev_wh and is_different_wh:
                self._log.debug("Switching back to warehouse: %s", prev_wh)
                self.use(object_type=ObjectType.WAREHOUSE, name=prev_wh)

    def create_password_secret(
        self, name: FQN, username: str, password: str
    ) -> SnowflakeCursor:
        return self.execute_query(
            f"""
            create secret {name.sql_identifier}
            type = password
            username = '{username}'
            password = '{password}'
            """
        )

    def create_api_integration(
        self, name: FQN, api_provider: str, allowed_prefix: str, secret: Optional[str]
    ) -> SnowflakeCursor:
        return self.execute_query(
            f"""
            create api integration {name.sql_identifier}
            api_provider = {api_provider}
            api_allowed_prefixes = ('{allowed_prefix}')
            allowed_authentication_secrets = ({secret if secret else ""})
            enabled = true
            """
        )

    def _execute_schema_query(self, query: str, name: Optional[str] = None, **kwargs):
        """
        Check that a database and schema are provided before executing the query. Useful for operating on schema level objects.
        """
        self.check_database_and_schema_provided(name)
        return self.execute_query(query, **kwargs)

    def check_database_and_schema_provided(self, name: Optional[str] = None) -> None:
        """
        Checks if a database and schema are provided, either through the connection context or a qualified name.
        """
        fqn = FQN.from_string(name).using_connection(self._conn)
        if not fqn.database:
            raise DatabaseNotProvidedError()
        if not fqn.schema:
            raise SchemaNotProvidedError()

    @staticmethod
    def _qualified_name_to_in_clause(identifier: FQN) -> Tuple[str, Optional[str]]:
        if identifier.database:
            schema = identifier.schema or "PUBLIC"
            in_clause = f"in schema {identifier.database}.{schema}"
        elif identifier.schema:
            in_clause = f"in schema {identifier.schema}"
        else:
            in_clause = None
        return identifier.name, in_clause

    class InClauseWithQualifiedNameError(ValueError):
        def __init__(self):
            super().__init__("non-empty 'in_clause' passed with qualified 'name'")

    def show_specific_object(
        self,
        object_type_plural: str,
        name: str,
        name_col: str = "name",
        in_clause: str = "",
        check_schema: bool = False,
    ) -> Optional[dict]:
        """
        Executes a "show <objects> like" query for a particular entity with a
        given (optionally qualified) name. This command is useful when the corresponding
        "describe <object>" query does not provide the information you seek.

        Note that this command is analogous to describe and should only return a single row.
        If the target object type is a schema level object, then check_schema should be set to True
        so that the function will verify that a database and schema are provided, either through
        the connection or a qualified name, before executing the query.
        """

        unqualified_name, name_in_clause = self._qualified_name_to_in_clause(
            FQN.from_string(name)
        )
        if in_clause and name_in_clause:
            raise self.InClauseWithQualifiedNameError()
        elif name_in_clause:
            in_clause = name_in_clause
        show_obj_query = f"show {object_type_plural} like {identifier_to_show_like_pattern(unqualified_name)} {in_clause}".strip()

        if check_schema:
            show_obj_cursor = self._execute_schema_query(  # type: ignore
                show_obj_query, name=name, cursor_class=DictCursor
            )
        else:
            show_obj_cursor = self.execute_query(  # type: ignore
                show_obj_query, cursor_class=DictCursor
            )

        if show_obj_cursor.rowcount is None:
            raise SnowflakeSQLExecutionError(show_obj_query)
        elif show_obj_cursor.rowcount > 1:
            raise ShowSpecificObjectMultipleRowsError(show_obj_query=show_obj_query)

        show_obj_row = find_first_row(
            show_obj_cursor,
            lambda row: row[name_col] == unquote_identifier(unqualified_name),
        )
        return show_obj_row

    def disable_autocommit(self):
        """Disable autocommit in current context."""
        return self._conn.autocommit(False)


class SqlExecutionMixin(SqlExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._snowpark_session = None

    @property
    def snowpark_session(self):
        if not self._snowpark_session:
            from snowflake.snowpark.session import Session

            self._snowpark_session = Session.builder.configs(
                {"connection": self._conn}
            ).create()
        return self._snowpark_session


class VerboseCursor(SnowflakeCursor):
    def execute(self, command: str, *args, **kwargs):
        cli_console.message(command)
        super().execute(command, *args, **kwargs)
