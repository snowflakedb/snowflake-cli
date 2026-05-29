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

from __future__ import annotations

from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import to_string_literal
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class ViewManager(SqlExecutionMixin):
    def show(self, *, like: str = "%%") -> SnowflakeCursor:
        return self.execute_query(f"show views like {to_string_literal(like)}")

    def create(self, *, name: FQN, query: str, replace: bool = False) -> SnowflakeCursor:
        or_replace = " or replace" if replace else ""
        return self.execute_query(
            f"create{or_replace} view {name.sql_identifier} as {query}"
        )
