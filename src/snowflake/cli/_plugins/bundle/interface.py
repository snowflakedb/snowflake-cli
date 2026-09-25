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

"""Command surface for ``snow bundle`` — Snowflake Code Bundles.

This module is the reviewable interface: it declares every command, parameter
and help string as plain data, plus the handler ABC that ``handler.py``
implements. See docs/contributing/writing-a-plugin.md.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import List, Optional, Tuple

from snowflake.cli.api.commands.flags import IdentifierType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import CommandResult
from snowflake.cli.api.plugins.command.interface import (
    CommandDef,
    CommandGroupSpec,
    CommandHandler,
    ParamDef,
    ParamKind,
)

# Shared by every command that names an existing or new code bundle. ParamDef is
# frozen, so the same instance can be reused across commands.
_IDENTIFIER = ParamDef(
    name="identifier",
    type=FQN,
    kind=ParamKind.ARGUMENT,
    help="Identifier of the code bundle; for example: MY_CODE_BUNDLE",
    show_default=False,
    click_type=IdentifierType(),
)

# `status` and `cancel` both act on a query ID handed out by `execute --async`.
_QUERY_ID = ParamDef(
    name="query_id",
    type=str,
    kind=ParamKind.ARGUMENT,
    help="Snowflake query ID returned by `bundle execute --async`.",
    show_default=False,
)

BUNDLE_SPEC = CommandGroupSpec(
    name="bundle",
    help="Manages Snowflake Code Bundles.",
    parent_path=(),  # () = attach at root: `snow bundle`
    commands=(
        CommandDef(
            name="create",
            help="Creates a code bundle.",
            handler_method="create",
            requires_connection=True,
            output_type="MessageResult",
            params=(
                _IDENTIFIER,
                ParamDef(
                    name="source",
                    type=str,
                    kind=ParamKind.OPTION,
                    cli_names=("--source", "-s"),
                    help=(
                        "Source location for the code bundle. Supports stage path "
                        "(starting with '@'), workspace path (starting with "
                        "'snow://workspace/'), or local file system path (starting "
                        "with 'file://' or no protocol prefix)."
                    ),
                    show_default=False,
                ),
                ParamDef(
                    name="comment",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--comment",),
                    help="Comment for the code bundle.",
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="overwrite",
                    type=bool,
                    kind=ParamKind.OPTION,
                    cli_names=("--overwrite",),
                    help=(
                        "Replace the code bundle if it already exists "
                        "(CREATE OR REPLACE)."
                    ),
                    is_flag=True,
                    default=False,
                ),
                ParamDef(
                    name="skip_if_exists",
                    type=bool,
                    kind=ParamKind.OPTION,
                    cli_names=("--skip-if-exists",),
                    help=(
                        "Skip creation if the code bundle already exists "
                        "(CREATE ... IF NOT EXISTS)."
                    ),
                    is_flag=True,
                    default=False,
                ),
                ParamDef(
                    name="exclude",
                    type=Optional[List[str]],
                    kind=ParamKind.OPTION,
                    cli_names=("--exclude",),
                    help=(
                        "Glob pattern for files or directories to exclude when "
                        "uploading a local source directory. Can be specified "
                        "multiple times. Patterns are matched against each path "
                        "component, so a pattern like 'venv' excludes any file or "
                        "directory named 'venv' at any depth (e.g. both /venv/ and "
                        "/dir/venv/), while '*.pyc' excludes all .pyc files "
                        "anywhere in the tree. Only applies when --source is a "
                        "local path; ignored for stage or workspace sources."
                    ),
                    default=None,
                    show_default=False,
                ),
            ),
        ),
        CommandDef(
            name="list",
            help="Lists code bundles.",
            handler_method="list_bundles",
            requires_connection=True,
            output_type="QueryResult",
            params=(
                ParamDef(
                    name="like",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--like", "-l"),
                    help=(
                        "SQL LIKE pattern for filtering code bundles by name. "
                        'For example, `list --like "my%"` lists all code bundles '
                        'that begin with "my".'
                    ),
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="scope",
                    type=Tuple[str, str],
                    kind=ParamKind.OPTION,
                    cli_names=("--in",),
                    help=(
                        "Scope of this command: '--in <scope> <name>' where scope "
                        "is 'database' or 'schema'. For example, '--in schema "
                        "mydb.myschema' or '--in database mydb'."
                    ),
                    default=(None, None),
                ),
                ParamDef(
                    name="in_account",
                    type=bool,
                    kind=ParamKind.OPTION,
                    cli_names=("--in-account",),
                    help="Lists code bundles across the entire account.",
                    is_flag=True,
                    default=False,
                ),
            ),
        ),
        CommandDef(
            name="delete",
            help="Drops a code bundle.",
            handler_method="delete",
            requires_connection=True,
            output_type="MessageResult",
            params=(
                _IDENTIFIER,
                ParamDef(
                    name="if_exists",
                    type=bool,
                    kind=ParamKind.OPTION,
                    cli_names=("--if-exists",),
                    help="Do nothing if the code bundle does not exist.",
                    is_flag=True,
                    default=False,
                ),
            ),
        ),
        CommandDef(
            name="alter",
            help="Alters a code bundle by renaming it or adding a new version.",
            handler_method="alter",
            requires_connection=True,
            output_type="MessageResult",
            params=(
                _IDENTIFIER,
                ParamDef(
                    name="rename_to",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--rename-to",),
                    help="New name for the code bundle.",
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="add_version",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--add-version",),
                    help=(
                        "Source location for a new version of the code bundle. "
                        "Supports stage path (starting with '@') or workspace path "
                        "(starting with 'snow://workspace/')."
                    ),
                    default=None,
                    show_default=False,
                ),
            ),
        ),
        CommandDef(
            name="execute",
            help=(
                "Executes a code bundle at the given entrypoint.\n\n"
                "Any additional arguments after the known options will be passed "
                "to the code bundle. For example: snow bundle execute my_bundle "
                "--entrypoint src/main.py -- --custom-arg value"
            ),
            handler_method="execute",
            requires_connection=True,
            output_type="MessageResult",
            # Unrecognised arguments are forwarded to the bundle rather than
            # rejected, and land in the variadic `arguments` parameter below.
            context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
            params=(
                _IDENTIFIER,
                ParamDef(
                    name="entrypoint",
                    type=str,
                    kind=ParamKind.OPTION,
                    cli_names=("--entrypoint",),
                    help="Entrypoint file path within the code bundle.",
                    show_default=False,
                ),
                ParamDef(
                    name="execution_name",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--execution-name",),
                    help="Name to assign to this code bundle execution.",
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="is_async",
                    type=bool,
                    kind=ParamKind.OPTION,
                    cli_names=("--async",),
                    help=(
                        "Run the bundle execution asynchronously and return the "
                        "query ID immediately."
                    ),
                    is_flag=True,
                    default=False,
                ),
                # Declared last so Click collects leftover tokens here, after the
                # bundle identifier has been consumed.
                ParamDef(
                    name="arguments",
                    type=Optional[List[str]],
                    kind=ParamKind.ARGUMENT,
                    help="Arguments forwarded to the code bundle.",
                    default=None,
                    show_default=False,
                ),
            ),
        ),
        CommandDef(
            name="status",
            help="Returns the execution status of an async code bundle execution.",
            handler_method="status",
            requires_connection=True,
            output_type="MessageResult",
            params=(_QUERY_ID,),
        ),
        CommandDef(
            name="cancel",
            help="Cancels an async code bundle execution.",
            handler_method="cancel",
            requires_connection=True,
            output_type="MessageResult",
            params=(_QUERY_ID,),
        ),
        CommandDef(
            name="history",
            help="Returns the execution history of code bundles.",
            handler_method="history",
            requires_connection=True,
            output_type="QueryResult",
            params=(
                ParamDef(
                    name="bundle_name",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--bundle-name",),
                    help="Filter the history to a specific code bundle name.",
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="bundle_database",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--bundle-database",),
                    help="Filter the history to code bundles in the given database.",
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="bundle_schema",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--bundle-schema",),
                    help="Filter the history to code bundles in the given schema.",
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="entrypoint",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--entrypoint",),
                    help="Filter the history to executions with the given entrypoint.",
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="start_time_range_start",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--start-time-range-start",),
                    help=(
                        "Filter the history to executions that started at or after "
                        "this timestamp."
                    ),
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="start_time_range_end",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--start-time-range-end",),
                    help=(
                        "Filter the history to executions that started at or before "
                        "this timestamp."
                    ),
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="bundle_types",
                    type=Optional[List[str]],
                    kind=ParamKind.OPTION,
                    cli_names=("--bundle-types",),
                    help=(
                        "Filter the history to the given bundle types. "
                        "Can be specified multiple times."
                    ),
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="compute_types",
                    type=Optional[List[str]],
                    kind=ParamKind.OPTION,
                    cli_names=("--compute-types",),
                    help=(
                        "Filter the history to the given compute types. "
                        "Can be specified multiple times."
                    ),
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="language_types",
                    type=Optional[List[str]],
                    kind=ParamKind.OPTION,
                    cli_names=("--language-types",),
                    help=(
                        "Filter the history to the given language types. "
                        "Can be specified multiple times."
                    ),
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="status",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--status",),
                    help="Filter the history to executions with the given status.",
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="execution_name",
                    type=Optional[str],
                    kind=ParamKind.OPTION,
                    cli_names=("--execution-name",),
                    help="Filter the history to the execution with the given name.",
                    default=None,
                    show_default=False,
                ),
                ParamDef(
                    name="result_limit",
                    type=int,
                    kind=ParamKind.OPTION,
                    cli_names=("--result-limit",),
                    help="Maximum number of history rows to return.",
                    default=100,
                ),
            ),
        ),
    ),
)


class BundleHandler(CommandHandler):
    """Contract for the ``snow bundle`` implementation.

    ``@abstractmethod`` is documentation here: ``CommandHandler`` declares no
    abstract members, and the real check runs in ``validate_interface_handler``
    at build time.
    """

    @abstractmethod
    def create(
        self,
        identifier: FQN,
        source: str,
        comment: Optional[str],
        overwrite: bool,
        skip_if_exists: bool,
        exclude: Optional[List[str]],
    ) -> CommandResult:
        ...

    @abstractmethod
    def list_bundles(
        self,
        like: Optional[str],
        scope: Tuple[str, str],
        in_account: bool,
    ) -> CommandResult:
        ...

    @abstractmethod
    def delete(self, identifier: FQN, if_exists: bool) -> CommandResult:
        ...

    @abstractmethod
    def alter(
        self,
        identifier: FQN,
        rename_to: Optional[str],
        add_version: Optional[str],
    ) -> CommandResult:
        ...

    @abstractmethod
    def execute(
        self,
        identifier: FQN,
        entrypoint: str,
        execution_name: Optional[str],
        is_async: bool,
        arguments: Optional[List[str]],
    ) -> CommandResult:
        ...

    @abstractmethod
    def status(self, query_id: str) -> CommandResult:
        ...

    @abstractmethod
    def cancel(self, query_id: str) -> CommandResult:
        ...

    @abstractmethod
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
        ...
