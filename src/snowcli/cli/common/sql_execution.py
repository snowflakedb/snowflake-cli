from __future__ import annotations

from textwrap import dedent

from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager


class SqlExecutionMixin:
    def __init__(self):
        pass

    @property
    def _conn(self):
        return snow_cli_global_context_manager.get_connection()

    def _execute_template(self, template_name: str, payload: dict):
        return self._conn.run_sql(template_name, payload)

    def _execute_query(self, query: str):
        *_, last_result = self._conn.ctx.execute_string(dedent(query))
        return last_result

    def _execute_queries(self, queries: str):
        return self._conn.ctx.execute_string(dedent(queries))
