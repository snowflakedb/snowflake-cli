from __future__ import annotations

from functools import cached_property

from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager


class SqlExecutionMixin:
    def __init__(self):
        pass

    @cached_property
    def _conn(self):
        return snow_cli_global_context_manager.get_connection()

    def _execute_template(self, template_name: str, payload: dict):
        return self._conn.run_sql(template_name, payload)

    def _execute_query(self, query: str):
        results = self._conn.ctx.execute_string(query)
        *_, last_result = results
        return last_result
