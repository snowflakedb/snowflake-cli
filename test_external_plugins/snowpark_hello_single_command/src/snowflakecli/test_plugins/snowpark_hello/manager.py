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
from snowflake.cli.api.plugins.plugin_config import PluginConfigProvider
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class SnowparkHelloManager(SqlExecutionMixin):

    _plugin_config_manager = PluginConfigProvider()
    _greeting = _plugin_config_manager.get_config("snowpark-hello").internal_config[
        "greeting"
    ]

    def say_hello(self, name: str) -> SnowflakeCursor:
        return self.execute_query(
            f"SELECT '{self._greeting} {name}! You are in Snowpark!' as greeting"
        )
