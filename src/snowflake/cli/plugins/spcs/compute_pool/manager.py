from typing import Optional

from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import ObjectAlreadyExistsError
from snowflake.cli.api.project.util import unquote_identifier
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.spcs.common import strip_empty_lines
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError


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

        try:
            return self._execute_query(strip_empty_lines(query))
        except ProgrammingError as e:
            if e.errno == 2002:
                raise ObjectAlreadyExistsError(
                    object_type=ObjectType.COMPUTE_POOL,
                    name=unquote_identifier(pool_name),
                )
            else:
                raise

    def stop(self, pool_name: str) -> SnowflakeCursor:
        return self._execute_query(f"alter compute pool {pool_name} stop all")

    def suspend(self, pool_name: str) -> SnowflakeCursor:
        return self._execute_query(f"alter compute pool {pool_name} suspend")

    def resume(self, pool_name: str) -> SnowflakeCursor:
        return self._execute_query(f"alter compute pool {pool_name} resume")
