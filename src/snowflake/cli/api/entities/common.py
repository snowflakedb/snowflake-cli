from snowflake.cli.api.sql_execution import SqlExecutor


def get_sql_executor() -> SqlExecutor:
    """Returns an SQL Executor that uses the connection from the current CLI context"""
    return SqlExecutor()
