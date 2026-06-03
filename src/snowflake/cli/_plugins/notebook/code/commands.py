# Copyright (c) 2025 Snowflake Inc.
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

from typing import Optional, Tuple

import typer
from snowflake.cli._plugins.notebook.code.manager import CodeBundleManager
from snowflake.cli._plugins.object.common import CommentOption
from snowflake.cli.api.commands.flags import IfExistsOption, identifier_argument
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.exceptions import CliError, IncompatibleParametersError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import CommandResult, MessageResult, QueryResult
from typing_extensions import Annotated

app = SnowTyperFactory(
    name="code",
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
        "Source location of the notebook project. Supports stage path "
        "(starting with '@') or workspace path "
        "(starting with 'snow://workspace/')."
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


@app.command(requires_connection=True)
def create(
    identifier: Annotated[FQN, CODE_BUNDLE_IDENTIFIER],
    source: Annotated[str, SourceOption],
    comment: Optional[str] = CommentOption(),
    overwrite: bool = OverwriteOption,
    skip_if_exists: bool = SkipIfExistsOption,
    **options,
) -> CommandResult:
    """Creates a code bundle for a notebook project."""
    if overwrite and skip_if_exists:
        raise IncompatibleParametersError(["--overwrite", "--skip-if-exists"])
    cursor = CodeBundleManager().create(
        name=identifier,
        source=source,
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
