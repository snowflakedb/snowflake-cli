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

import fnmatch
import glob
import os
import random
from pathlib import Path
from typing import List, Optional, Tuple, Union

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

    def execute(
        self,
        name: FQN,
        entrypoint: str,
        arguments: Optional[List[str]] = None,
        run_async: bool = False,
    ) -> SnowflakeCursor:
        if name is None or not name.name:
            raise CliError("Code bundle name is required.")
        if not entrypoint:
            raise CliError("Entrypoint is required.")
        fqn = name.using_connection(self._conn)
        query = (
            f"EXECUTE CODE BUNDLE {fqn.sql_identifier} "
            f"ENTRYPOINT={to_string_literal(entrypoint)}"
        )
        if arguments:
            formatted_args = ", ".join(to_string_literal(arg) for arg in arguments)
            query += f" ARGUMENTS=({formatted_args})"
        return self.execute_query(query, _exec_async=run_async)

    def get_status(self, query_id: str) -> str:
        if not query_id:
            raise CliError("Query ID is required.")
        try:
            status = self._conn.get_query_status(query_id)
        except ValueError as e:
            raise CliError(f"Invalid query ID: {query_id}") from e
        return status.name

    def cancel(self, query_id: str) -> SnowflakeCursor:
        if not query_id:
            raise CliError("Query ID is required.")
        return self._conn.cursor().execute(
            "SELECT SYSTEM$CANCEL_QUERY(%s)", (query_id,)
        )

    def history(self, name: FQN, result_limit: int = 100) -> SnowflakeCursor:
        if name is None or not name.name:
            raise CliError("Code bundle name is required.")
        fqn = name.using_connection(self._conn)
        query = (
            "SELECT * FROM TABLE("
            "SNOWFLAKE.INFORMATION_SCHEMA.CODE_BUNDLE_HISTORY("
            f"BUNDLE_NAME => {to_string_literal(fqn.identifier)}, "
            f"RESULT_LIMIT => {int(result_limit)}))"
        )
        return self.execute_query(query)

    def process_source(self, source: str, exclude: Optional[List[str]] = None) -> str:
        """Resolve a user-provided --source value.

        Returns the source unchanged for stage paths (``@...``) and workspace
        paths (``snow://...``). For local paths (``file://...`` or no protocol
        prefix), creates a temporary stage, uploads the directory recursively,
        and returns the resulting stage path.
        """
        if source.startswith("@"):
            return source
        source_lower = source.lower()
        if source_lower.startswith("snow://"):
            return source
        if source_lower.startswith("file://") or "://" not in source:
            local_path = source[7:] if source_lower.startswith("file://") else source
            stage_name = f"tmp_bundle_stage_{random.randint(1000000, 9999999)}"
            self.execute_query(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")
            self._upload_directory_recursive(local_path, stage_name, exclude=exclude)
            return f"@{stage_name}"
        raise CliError(
            f"Invalid source: '{source}'. Source must be a Snowflake stage "
            "path (starting with '@'), a Snowflake workspace path (starting "
            "with 'snow://'), or a local file system path (starting with "
            "'file://' or no protocol prefix)."
        )

    def _upload_directory_recursive(
        self,
        local_path: str,
        stage_name: str,
        exclude: Optional[List[str]] = None,
    ) -> None:
        """Upload all files from local_path to stage, preserving directory structure."""
        root_path = Path(local_path)
        if not root_path.is_dir():
            raise CliError(f"Source path '{local_path}' is not a directory.")
        glob_pattern = os.path.join(glob.escape(local_path), "**", "*")

        for file_path_str in glob.iglob(glob_pattern, recursive=True):
            file_path = Path(file_path_str)
            if not file_path.is_file():
                continue
            if exclude and any(
                fnmatch.fnmatchcase(part, pattern)
                for part in file_path.parts
                for pattern in exclude
            ):
                continue
            relative_path = file_path.relative_to(root_path)
            stage_subdir = str(relative_path.parent)
            if stage_subdir == ".":
                stage_dest = f"@{stage_name}"
            else:
                stage_dest = f"@{stage_name}/{stage_subdir}"
            self.execute_query(
                f"PUT file://{file_path} {stage_dest} auto_compress=false"
            )
