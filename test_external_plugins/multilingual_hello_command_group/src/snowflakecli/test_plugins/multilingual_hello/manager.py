# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        return self.execute_query(f"SELECT '{prefix} {name}!' as greeting")
