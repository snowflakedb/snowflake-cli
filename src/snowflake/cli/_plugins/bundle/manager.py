# Copyright (c) 2026 Snowflake Inc.
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

from typing import Optional, Tuple, Union

from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import to_string_literal
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class CodeBundleManager(SqlExecutionMixin):
    """Manages Snowflake Code Bundles."""

    def create(
        self,
        name: FQN,
        source: str,
        comment: Optional[str] = None,
        overwrite: bool = False,
        skip_if_exists: bool = False,
    ) -> SnowflakeCursor:
        if name is None or not name.name:
            raise CliError("Code bundle name is required.")
        if not source:
            raise CliError("Source is required.")

        fqn = name.using_connection(self._conn)
        if overwrite:
            create_clause = "CREATE OR REPLACE CODE BUNDLE"
        elif skip_if_exists:
            create_clause = "CREATE CODE BUNDLE IF NOT EXISTS"
        else:
            create_clause = "CREATE CODE BUNDLE"
        query = (
            f"{create_clause} {fqn.sql_identifier} " f"FROM {to_string_literal(source)}"
        )
        if comment is not None:
            query += f" COMMENT = {comment}"
        return self.execute_query(query)

    def show(
        self,
        like: Optional[str] = None,
        scope: Union[Tuple[str, str], Tuple[None, None]] = (None, None),
        in_account: bool = False,
    ) -> SnowflakeCursor:
        query = "SHOW CODE BUNDLES"
        if like is not None:
            query += f" LIKE {to_string_literal(like)}"
        if in_account:
            query += " IN ACCOUNT"
        elif scope[0] is not None:
            scope_type = scope[0].upper()
            scope_name = FQN.from_string(scope[1]).sql_identifier
            query += f" IN {scope_type} {scope_name}"
        return self.execute_query(query)

    def drop(self, name: FQN, if_exists: bool = False) -> SnowflakeCursor:
        if name is None or not name.name:
            raise CliError("Code bundle name is required.")
        fqn = name.using_connection(self._conn)
        if_exists_clause = "IF EXISTS " if if_exists else ""
        query = f"DROP CODE BUNDLE {if_exists_clause}{fqn.sql_identifier}"
        return self.execute_query(query)

    def alter(
        self,
        name: FQN,
        rename_to: Optional[str] = None,
        add_version: Optional[str] = None,
    ) -> SnowflakeCursor:
        if name is None or not name.name:
            raise CliError("Code bundle name is required.")
        fqn = name.using_connection(self._conn)
        query = f"ALTER CODE BUNDLE {fqn.sql_identifier}"
        if rename_to is not None:
            new_fqn = FQN.from_string(rename_to).using_connection(self._conn)
            query += f" RENAME TO {new_fqn.sql_identifier}"
        elif add_version is not None:
            query += f" ADD VERSION FROM {to_string_literal(add_version)}"
        return self.execute_query(query)

    def execute(self, name: FQN, entrypoint: str) -> SnowflakeCursor:
        if name is None or not name.name:
            raise CliError("Code bundle name is required.")
        if not entrypoint:
            raise CliError("Entrypoint is required.")
        fqn = name.using_connection(self._conn)
        query = (
            f"EXECUTE CODE BUNDLE {fqn.sql_identifier} "
            f"ENTRYPOINT={to_string_literal(entrypoint)}"
        )
        return self.execute_query(query)
