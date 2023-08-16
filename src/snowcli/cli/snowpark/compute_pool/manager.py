from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.sql_execution import SqlExecutionMixin


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

    def show(self) -> SnowflakeCursor:
        return self._execute_query("show compute pools;")

    def drop(
        self,
        pool_name: str,
    ) -> SnowflakeCursor:
        return self._execute_query(f"drop compute pool {pool_name};")

    def stop(self, pool_name: str) -> SnowflakeCursor:
        return self._execute_query(f"alter compute pool {pool_name} stop all;")
