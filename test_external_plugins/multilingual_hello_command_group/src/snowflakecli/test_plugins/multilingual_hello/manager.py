from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor
from snowflakecli.test_plugins.multilingual_hello.hello_language import HelloLanguage


class MultilingualHelloManager(SqlExecutionMixin):
    def say_hello(self, name: str, language: HelloLanguage) -> SnowflakeCursor:
        prefix = "Hello"
        if language == HelloLanguage.en:
            prefix = "Hello"
        elif language == HelloLanguage.fr:
            prefix = "Salut"
        elif language == HelloLanguage.de:
            prefix = "Hallo"
        elif language == HelloLanguage.pl:
            prefix = "Czesc"
        return self._execute_query(f"SELECT '{prefix} {name}!' as greeting")
