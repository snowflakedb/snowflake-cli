from snowflake.cli._plugins.nativeapp.sf_sql_facade import SnowflakeSQLFacade


def get_snowflake_facade() -> SnowflakeSQLFacade:
    """Returns a Snowflake Facade"""
    return SnowflakeSQLFacade()
