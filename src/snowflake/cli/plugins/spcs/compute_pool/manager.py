from typing import Optional

from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.spcs.common import strip_empty_lines
from snowflake.connector.cursor import SnowflakeCursor


class ComputePoolManager(SqlExecutionMixin):
    def create(
        self,
        pool_name: str,
        min_nodes: int,
        max_nodes: int,
        instance_family: str,
        auto_resume: bool,
        initially_suspended: bool,
        auto_suspend_secs: int,
        comment: Optional[str],
    ) -> SnowflakeCursor:
        query = f"""\
            CREATE COMPUTE POOL {pool_name}
            MIN_NODES = {min_nodes}
            MAX_NODES = {max_nodes}
            INSTANCE_FAMILY = {instance_family}
            AUTO_RESUME = {auto_resume}
            INITIALLY_SUSPENDED = {initially_suspended}
            AUTO_SUSPEND_SECS = {auto_suspend_secs}
            """.splitlines()
        if comment:
            query.append(f"COMMENT = {comment}")
        return self._execute_query(strip_empty_lines(query))

    def stop(self, pool_name: str) -> SnowflakeCursor:
        return self._execute_query(f"alter compute pool {pool_name} stop all;")
