from snowflake.cli._plugins.nativeapp.sf_sql_facade import SnowflakeSQLFacade
from snowflake.cli.api.sql_execution import SqlExecutor


def get_snowflake_facade(sql_executor: SqlExecutor | None) -> SnowflakeSQLFacade:
    """Returns a Snowflake Facade"""
    return SnowflakeSQLFacade(sql_executor)
