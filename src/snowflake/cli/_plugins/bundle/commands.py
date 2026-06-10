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

from typing import List, Optional, Tuple

import typer
from snowflake.cli._plugins.bundle.manager import CodeBundleManager
from snowflake.cli._plugins.object.common import CommentOption
from snowflake.cli.api.commands.flags import IfExistsOption, identifier_argument
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.exceptions import CliError, IncompatibleParametersError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import CommandResult, MessageResult, QueryResult
from typing_extensions import Annotated

app = SnowTyperFactory(
    name="bundle",
    help="Manages Snowflake Code Bundles.",
)

CODE_BUNDLE_IDENTIFIER = identifier_argument(
    sf_object="code bundle", example="MY_CODE_BUNDLE"
)
SourceOption = typer.Option(
    ...,
    "--source",
    "-s",
    help=(
        "Source location for the code bundle. Supports stage path (starting "
        "with '@'), workspace path (starting with 'snow://workspace/'), or "
        "local file system path (starting with 'file://' or no protocol prefix)."
    ),
    show_default=False,
)
OverwriteOption = typer.Option(
    False,
    "--overwrite",
    help="Replace the code bundle if it already exists (CREATE OR REPLACE).",
)
SkipIfExistsOption = typer.Option(
    False,
    "--skip-if-exists",
    help="Skip creation if the code bundle already exists (CREATE ... IF NOT EXISTS).",
)
ExcludeOption = typer.Option(
    None,
    "--exclude",
    help=(
        "Glob pattern for files or directories to exclude when uploading a "
        "local source directory. Can be specified multiple times. Patterns "
        "are matched against each path component, so a pattern like 'venv' "
        "excludes any file or directory named 'venv' at any depth (e.g. both "
        "/venv/ and /dir/venv/), while '*.pyc' excludes all .pyc files "
        "anywhere in the tree. Only applies when --source is a local path; "
        "ignored for stage or workspace sources."
    ),
    show_default=False,
)
LikeOption = typer.Option(
    None,
    "--like",
    "-l",
    help=(
        "SQL LIKE pattern for filtering code bundles by name. "
        'For example, `list --like "my%"` lists all code bundles that begin with "my".'
    ),
    show_default=False,
)
InOption = typer.Option(
    (None, None),
    "--in",
    help=(
        "Scope of this command: '--in <scope> <name>' where scope is 'database' or 'schema'. "
        "For example, '--in schema mydb.myschema' or '--in database mydb'."
    ),
)
InAccountOption = typer.Option(
    False,
    "--in-account",
    help="Lists code bundles across the entire account.",
)
RenameToOption = typer.Option(
    None,
    "--rename-to",
    help="New name for the code bundle.",
    show_default=False,
)
AddVersionOption = typer.Option(
    None,
    "--add-version",
    help=(
        "Source location for a new version of the code bundle. Supports stage path "
        "(starting with '@') or workspace path (starting with 'snow://workspace/')."
    ),
    show_default=False,
)
EntrypointOption = typer.Option(
    ...,
    "--entrypoint",
    help="Entrypoint file path within the code bundle.",
    show_default=False,
)
AsyncOption = typer.Option(
    False,
    "--async",
    help="Run the bundle execution asynchronously and return the query ID immediately.",
)
QueryIdArgument = typer.Argument(
    ...,
    help="Snowflake query ID returned by `bundle execute --async`.",
    show_default=False,
)


@app.command(requires_connection=True)
def create(
    identifier: Annotated[FQN, CODE_BUNDLE_IDENTIFIER],
    source: Annotated[str, SourceOption],
    comment: Optional[str] = CommentOption(),
    overwrite: bool = OverwriteOption,
    skip_if_exists: bool = SkipIfExistsOption,
    exclude: Optional[List[str]] = ExcludeOption,
    **options,
) -> CommandResult:
    """Creates a code bundle for a notebook project."""
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


@app.command(name="list", requires_connection=True)
def list_cmd(
    like: Optional[str] = LikeOption,
    scope: Tuple[str, str] = InOption,
    in_account: bool = InAccountOption,
    **options,
) -> CommandResult:
    """Lists code bundles."""
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


@app.command(requires_connection=True)
def delete(
    identifier: Annotated[FQN, CODE_BUNDLE_IDENTIFIER],
    if_exists: bool = IfExistsOption(
        help="Do nothing if the code bundle does not exist."
    ),
    **options,
) -> CommandResult:
    """Drops a code bundle."""
    cursor = CodeBundleManager().drop(name=identifier, if_exists=if_exists)
    return MessageResult(cursor.fetchone()[0])


@app.command(requires_connection=True)
def alter(
    identifier: Annotated[FQN, CODE_BUNDLE_IDENTIFIER],
    rename_to: Optional[str] = RenameToOption,
    add_version: Optional[str] = AddVersionOption,
    **options,
) -> CommandResult:
    """Alters a code bundle by renaming it or adding a new version."""
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


@app.command(
    requires_connection=True,
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def execute(
    ctx: typer.Context,
    identifier: Annotated[FQN, CODE_BUNDLE_IDENTIFIER],
    entrypoint: Annotated[str, EntrypointOption],
    is_async: bool = AsyncOption,
    **options,
) -> CommandResult:
    """Executes a code bundle at the given entrypoint.

    Any additional arguments after the known options will be passed to the code bundle.
    For example: snow bundle execute my_bundle --entrypoint src/main.py -- --custom-arg value
    """
    arguments = ctx.args if ctx.args else None
    cursor = CodeBundleManager().execute(
        name=identifier,
        entrypoint=entrypoint,
        arguments=arguments,
        run_async=is_async,
    )
    if is_async:
        return MessageResult(f"Request submitted. Query ID: {cursor.sfqid}")
    return MessageResult(cursor.fetchone()[0])


@app.command(requires_connection=True)
def status(
    query_id: str = QueryIdArgument,
    **options,
) -> CommandResult:
    """Returns the execution status of an async code bundle execution."""
    status_name = CodeBundleManager().get_status(query_id=query_id)
    return MessageResult(f"Query {query_id}: {status_name}")
