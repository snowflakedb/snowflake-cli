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

import sys
from enum import Enum
from logging import getLogger
from pathlib import Path
from typing import List, Optional

import typer
from click import UsageError
from snowflake.cli._plugins.sql.manager import SqlManager
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import (
    variables_option,
)
from snowflake.cli.api.commands.overrideable_parameter import OverrideableOption
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.exceptions import CliArgumentError
from snowflake.cli.api.output.types import (
    CommandResult,
    MultipleResults,
    QueryResult,
)
from snowflake.cli.api.rendering.sql_templates import SQLTemplateSyntaxConfig

logger = getLogger(__name__)
# simple Typer with defaults because it won't become a command group as it contains only one command
app = SnowTyperFactory()

SOURCE_EXCLUSIVE_OPTIONS_NAMES = ["query", "files", "std_in"]

SourceOption = OverrideableOption(
    mutually_exclusive=SOURCE_EXCLUSIVE_OPTIONS_NAMES, show_default=False
)


class _EnabledTemplating(str, Enum):
    LEGACY = "LEGACY"
    STANDARD = "STANDARD"
    JINJA = "JINJA"
    ALL = "ALL"
    NONE = "NONE"


def _parse_template_syntax_config(
    enabled_syntaxes: List[_EnabledTemplating],
) -> SQLTemplateSyntaxConfig:
    if (
        _EnabledTemplating.ALL in enabled_syntaxes
        or _EnabledTemplating.NONE in enabled_syntaxes
    ) and len(enabled_syntaxes) > 1:
        raise UsageError(
            "ALL and NONE template syntax options should not be used with other options."
        )

    if _EnabledTemplating.ALL in enabled_syntaxes:
        return SQLTemplateSyntaxConfig(True, True, True)
    if _EnabledTemplating.NONE in enabled_syntaxes:
        return SQLTemplateSyntaxConfig(False, False, False)

    result = SQLTemplateSyntaxConfig()
    result.enable_legacy_syntax = _EnabledTemplating.LEGACY in enabled_syntaxes
    result.enable_standard_syntax = _EnabledTemplating.STANDARD in enabled_syntaxes
    result.enable_jinja_syntax = _EnabledTemplating.JINJA in enabled_syntaxes
    return result


@app.command(name="sql", requires_connection=True, no_args_is_help=False)
@with_project_definition(is_optional=True)
def execute_sql(
    query: Optional[str] = SourceOption(
        default=None,
        param_decls=["--query", "-q"],
        help="Query to execute.",
    ),
    files: Optional[List[Path]] = SourceOption(
        default=[],
        param_decls=["--filename", "-f"],
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="File to execute.",
    ),
    std_in: Optional[bool] = SourceOption(
        default=False,
        param_decls=["--stdin", "-i"],
        help="Read the query from standard input. Use it when piping input to this command.",
    ),
    data_override: List[str] = variables_option(
        "String in format of key=value. If provided the SQL content will "
        "be treated as template and rendered using provided data.",
    ),
    retain_comments: Optional[bool] = typer.Option(
        False,
        "--retain-comments",
        help="Retains comments in queries passed to Snowflake",
    ),
    single_transaction: Optional[bool] = typer.Option(
        False,
        help="Connects with autocommit disabled. Wraps BEGIN/COMMIT around statements to execute them as a single transaction, ensuring all commands complete successfully or no change is applied.",
        flag_value=False,
        is_flag=True,
    ),
    enabled_templating: List[_EnabledTemplating] = typer.Option(
        [_EnabledTemplating.LEGACY, _EnabledTemplating.STANDARD],
        "--enable-templating",
        help="Syntax used to resolve variables before passing queries to Snowflake.",
        case_sensitive=False,
    ),
    **options,
) -> CommandResult:
    """
    Executes Snowflake query.

    Use either query, filename or input option.

    Query to execute can be specified using query option, filename option (all queries from file will be executed)
    or via stdin by piping output from other command. For example `cat my.sql | snow sql -i`.

    The command supports variable substitution that happens on client-side.
    """

    data = {}
    if data_override:
        data = {v.key: v.value for v in parse_key_value_variables(data_override)}

    template_syntax_config = _parse_template_syntax_config(enabled_templating)

    retain_comments = bool(retain_comments)
    single_transaction = bool(single_transaction)
    std_in = bool(std_in)

    no_source_provided = not any([query, files, std_in])
    if no_source_provided and not sys.stdin.isatty():
        maybe_pipe = sys.stdin.read().strip()
        if maybe_pipe:
            query = maybe_pipe
            std_in = True

    if no_source_provided:
        if single_transaction:
            raise CliArgumentError("single transaction cannot be used with REPL")
        from snowflake.cli._plugins.sql.repl import Repl

        Repl(
            SqlManager(),
            data=data,
            retain_comments=retain_comments,
            template_syntax_config=template_syntax_config,
        ).run()
        sys.exit(0)

    manager = SqlManager()

    expected_results_cnt, cursors = manager.execute(
        query,
        files,
        std_in,
        data=data,
        retain_comments=retain_comments,
        single_transaction=single_transaction,
        template_syntax_config=template_syntax_config,
    )
    if expected_results_cnt == 0:
        # case expected if input only scheduled async queries
        list(cursors)  # evaluate the result to schedule potential async queries
        # ends gracefully with no message for consistency with snowsql.
        sys.exit(0)

    if expected_results_cnt == 1:
        # evaluate the result to schedule async queries
        results = list(cursors)
        if not results:
            return sys.exit(0)
        return QueryResult(results[0])

    return MultipleResults((QueryResult(c) for c in cursors))
