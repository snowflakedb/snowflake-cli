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

from contextvars import ContextVar

from snowflake.cli._plugins.nativeapp.sf_sql_facade import SnowflakeSQLFacade

_SNOWFLAKE_FACADE: ContextVar[SnowflakeSQLFacade | None] = ContextVar(
    "snowflake_sql_facade", default=None
)


def get_snowflake_facade() -> SnowflakeSQLFacade:
    """Returns a Snowflake Facade"""
    facade = _SNOWFLAKE_FACADE.get()
    if not facade:
        facade = SnowflakeSQLFacade()
        _SNOWFLAKE_FACADE.set(facade)
    return facade
