from snowflake.cli.api import api_provider
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class SnowparkHelloManager(SqlExecutionMixin):

    _api = api_provider.api()
    _greeting = _api.plugin_config_provider.get_config(
        "snowpark-hello"
    ).internal_config["greeting"]

    def say_hello(self, name: str) -> SnowflakeCursor:
        return self._execute_query(
            f"SELECT '{self._greeting} {name}! You are in Snowpark!' as greeting"
        )
