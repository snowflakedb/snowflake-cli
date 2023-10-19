from __future__ import annotations

from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.sql_execution import SqlExecutionMixin


class WarehouseManager(SqlExecutionMixin):
    def show(self) -> SnowflakeCursor:
        return self._execute_query("show warehouses")
