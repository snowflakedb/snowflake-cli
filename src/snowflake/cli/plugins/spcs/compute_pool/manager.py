from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class ComputePoolManager(SqlExecutionMixin):
    def create(
        self, pool_name: str, num_instances: int, instance_family: str
    ) -> SnowflakeCursor:
        return self._execute_query(
            f"""\
            CREATE COMPUTE POOL {pool_name}
            MIN_NODES = {num_instances}
            MAX_NODES = {num_instances}
            INSTANCE_FAMILY = {instance_family};
        """
        )

    def stop(self, pool_name: str) -> SnowflakeCursor:
        return self._execute_query(f"alter compute pool {pool_name} stop all;")
