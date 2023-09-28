from __future__ import annotations
from contextlib import contextmanager

from textwrap import dedent

from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager
from snowflake.connector.cursor import DictCursor


class SqlExecutionMixin:
    def __init__(self):
        pass

    @property
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
        yield
        if is_different_role:
            self._execute_query(f"use role {prev_role}")
