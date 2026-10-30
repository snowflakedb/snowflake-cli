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

"""Implementation behind the ``snow bundle`` command surface.

Each method validates the parameter combinations the CLI cannot express on its
own, then delegates the SQL to ``CodeBundleManager``.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from snowflake.cli._plugins.bundle.interface import BundleHandler
from snowflake.cli._plugins.bundle.manager import CodeBundleManager
from snowflake.cli.api.exceptions import CliError, IncompatibleParametersError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import CommandResult, MessageResult, QueryResult


class CodeBundleHandler(BundleHandler):
    def create(
        self,
        identifier: FQN,
        source: str,
        comment: Optional[str],
        overwrite: bool,
        skip_if_exists: bool,
        exclude: Optional[List[str]],
    ) -> CommandResult:
        if overwrite and skip_if_exists:
            raise IncompatibleParametersError(["--overwrite", "--skip-if-exists"])
        if not source:
            raise CliError("Source is required.")
        manager = CodeBundleManager()
        processed_source = manager.process_source(source, exclude=exclude)
        cursor = manager.create(
            name=identifier,
            source=processed_source,
            comment=comment,
            overwrite=overwrite,
            skip_if_exists=skip_if_exists,
        )
        return MessageResult(cursor.fetchone()[0])

    def list_bundles(
        self,
        like: Optional[str],
        scope: Tuple[str, str],
        in_account: bool,
    ) -> CommandResult:
        if in_account and scope[0] is not None:
            raise IncompatibleParametersError(["--in-account", "--in"])
        if scope[0] is not None:
            if scope[0].lower() not in {"database", "schema"}:
                raise CliError("Scope must be 'database' or 'schema'.")
            if not scope[1]:
                raise CliError("Scope name cannot be empty.")
        return QueryResult(
            CodeBundleManager().show(like=like, scope=scope, in_account=in_account)
        )

    def delete(self, identifier: FQN, if_exists: bool) -> CommandResult:
        cursor = CodeBundleManager().drop(name=identifier, if_exists=if_exists)
        return MessageResult(cursor.fetchone()[0])

    def alter(
        self,
        identifier: FQN,
        rename_to: Optional[str],
        add_version: Optional[str],
    ) -> CommandResult:
        if rename_to is not None and add_version is not None:
            raise IncompatibleParametersError(["--rename-to", "--add-version"])
        if rename_to is None and add_version is None:
            raise CliError(
                "Exactly one of '--rename-to' or '--add-version' must be provided."
            )
        cursor = CodeBundleManager().alter(
            name=identifier,
            rename_to=rename_to,
            add_version=add_version,
        )
        return MessageResult(cursor.fetchone()[0])

    def execute(
        self,
        identifier: FQN,
        entrypoint: str,
        execution_name: Optional[str],
        is_async: bool,
        arguments: Optional[List[str]],
    ) -> CommandResult:
        # Click hands over an empty tuple when nothing was forwarded; the
        # manager distinguishes "no ARGUMENTS clause" by None.
        cursor = CodeBundleManager().execute(
            name=identifier,
            entrypoint=entrypoint,
            execution_name=execution_name,
            arguments=list(arguments) if arguments else None,
            run_async=is_async,
        )
        if is_async:
            return MessageResult(f"Request submitted. Query ID: {cursor.sfqid}")
        return MessageResult(cursor.fetchone()[0])

    def status(self, query_id: str) -> CommandResult:
        status_name = CodeBundleManager().get_status(query_id=query_id)
        return MessageResult(f"Query {query_id}: {status_name}")

    def cancel(self, query_id: str) -> CommandResult:
        cursor = CodeBundleManager().cancel(query_id=query_id)
        return MessageResult(cursor.fetchone()[0])

    def history(
        self,
        bundle_name: Optional[str],
        bundle_database: Optional[str],
        bundle_schema: Optional[str],
        entrypoint: Optional[str],
        start_time_range_start: Optional[str],
        start_time_range_end: Optional[str],
        bundle_types: Optional[List[str]],
        compute_types: Optional[List[str]],
        language_types: Optional[List[str]],
        status: Optional[str],
        execution_name: Optional[str],
        result_limit: int,
    ) -> CommandResult:
        return QueryResult(
            CodeBundleManager().history(
                bundle_name=bundle_name,
                database=bundle_database,
                schema=bundle_schema,
                entrypoint=entrypoint,
                start_time_range_start=start_time_range_start,
                start_time_range_end=start_time_range_end,
                bundle_types=bundle_types,
                compute_types=compute_types,
                language_types=language_types,
                status=status,
                execution_name=execution_name,
                result_limit=result_limit,
            )
        )
