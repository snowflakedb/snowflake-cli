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

import itertools
from os import path
from pathlib import Path
from typing import List, Optional

import click
import typer
from snowflake.cli._plugins.object.command_aliases import (
    add_object_command_aliases,
    scope_option,
)
from snowflake.cli._plugins.stage.diff import (
    DiffResult,
    compute_stage_diff,
)
from snowflake.cli._plugins.stage.manager import (
    InternalStageEncryptionType,
    StageManager,
)
from snowflake.cli._plugins.stage.utils import print_diff_to_console
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.command_docs import (
    PYTHON_EXECUTE_VERSION_SUPPORT,
    CommandDocs,
    Example,
    bullet,
    bullet_list,
    code,
    link,
    plain_text,
)
from snowflake.cli.api.commands.common import OnErrorType
from snowflake.cli.api.commands.flags import (
    ExecuteVariablesOption,
    OnErrorOption,
    PatternOption,
    identifier_stage_argument,
    identifier_stage_path_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    ObjectResult,
    QueryResult,
    SingleQueryResult,
)
from snowflake.cli.api.utils.path_utils import is_stage_path

app = SnowTyperFactory(
    name="stage",
    help="Manages stages.",
)

StageNameArgument = identifier_stage_argument(sf_object="stage", example="@my_stage")
StagePathArgument = identifier_stage_path_argument(
    sf_object="stage", example="@my_stage/path"
)


add_object_command_aliases(
    app=app,
    object_type=ObjectType.STAGE,
    name_argument=StageNameArgument,
    like_option=like_option(
        help_example='`list --like "my%"` lists all stages that begin with "my"',
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
)


@app.command("list-files", requires_connection=True)
def stage_list_files(
    stage_name: str = StagePathArgument, pattern=PatternOption, **options
) -> CommandResult:
    """
    Lists the stage contents.
    """
    cursor = StageManager().list_files(stage_name=stage_name, pattern=pattern)
    return QueryResult(cursor)


@app.command(
    "copy",
    requires_connection=True,
    docs=CommandDocs(
        related=(
            link("/developer-guide/snowflake-cli/index"),
            link(
                "/developer-guide/snowflake-cli/command-reference/overview",
                "Snowflake CLI command reference",
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/stage-commands/overview"
            ),
            link("/developer-guide/snowflake-cli/stages/manage-stages"),
        ),
        usage_notes=(
            plain_text(
                "One of ",
                code("SOURCE_PATH"),
                " or ",
                code("DESTINATION_PATH"),
                " must be a local directory, while the other should be a directory "
                "in the Snowflake stage. The stage path must start with ",
                code("@"),
                ". For example:",
            ),
            bullet_list(
                bullet(
                    code("snow stage copy @my_stage dir/"),
                    " - copies files from ",
                    code("my_stage"),
                    " stage to the local ",
                    code("dir"),
                    " directory.",
                ),
                bullet(
                    code("snow stage copy dir/ @my_stage"),
                    " - copies files from the local ",
                    code("dir"),
                    " directory to ",
                    code("my_stage"),
                    ".",
                ),
            ),
            plain_text(
                "You can specify multiple files matching a regular expression by using "
                "a glob pattern for the ",
                code("source_path"),
                " argument. You must enclose the glob pattern in single or double "
                "quotes.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "To copy files from the local machine to a stage, use a command "
                    "similar to the following:"
                ),
                command="snow stage copy local_example_app @example_app_stage/app",
                output=(
                    "put file:///.../local_example_app/* @example_app_stage/app4 "
                    "auto_compress=false parallel=4 overwrite=False\n"
                    "+--------------------------------------------------------------------------------------\n"
                    "| source           | target           | source_size | target_size | "
                    "source_compression...\n"
                    "|------------------+------------------+-------------+-------------+"
                    "--------------------\n"
                    "| environment.yml  | environment.yml  | 62          | 0           | "
                    "NONE             ...\n"
                    "| snowflake.yml    | snowflake.yml    | 252         | 0           | "
                    "NONE             ...\n"
                    "| streamlit_app.py | streamlit_app.py | 109         | 0           | "
                    "NONE             ...\n"
                    "+--------------------------------------------------------------------------------------"
                ),
            ),
            Example(
                description=plain_text(
                    "To download files from a stage to a local directory, use a command "
                    "similar to the following:"
                ),
                command=(
                    "mkdir local_app_backup\n"
                    "snow stage copy @example_app_stage/app local_app_backup"
                ),
                output=(
                    "get @example_app_stage/app file:///.../local_app_backup/ parallel=4\n"
                    "+------------------------------------------------+\n"
                    "| file             | size | status     | message |\n"
                    "|------------------+------+------------+---------|\n"
                    "| environment.yml  | 62   | DOWNLOADED |         |\n"
                    "| snowflake.yml    | 252  | DOWNLOADED |         |\n"
                    "| streamlit_app.py | 109  | DOWNLOADED |         |\n"
                    "+------------------------------------------------+"
                ),
            ),
            Example(
                description=plain_text(
                    "The following example copies all ",
                    code(".txt"),
                    " files in a directory to a stage.",
                ),
                command='snow stage copy "testdir/*.txt" @TEST_STAGE_3',
                output=(
                    "put file:///.../testdir/*.txt @TEST_STAGE_3 auto_compress=false "
                    "parallel=4 overwrite=False\n"
                    "+------------------------------------------------------------------------------------------------------------+\n"
                    "| source | target | source_size | target_size | source_compression | "
                    "target_compression | status   | message |\n"
                    "|--------+--------+-------------+-------------+--------------------+"
                    "--------------------+----------+---------|\n"
                    "| b1.txt | b1.txt | 3           | 16          | NONE               "
                    "| NONE               | UPLOADED |         |\n"
                    "| b2.txt | b2.txt | 3           | 16          | NONE               "
                    "| NONE               | UPLOADED |         |\n"
                    "+------------------------------------------------------------------------------------------------------------+"
                ),
            ),
        ),
    ),
)
def copy(
    source_path: str = typer.Argument(
        help="Source path for copy operation. Can be either stage path or local. You can use a glob pattern for local files but the pattern has to be enclosed in quotes.",
        show_default=False,
    ),
    destination_path: str = typer.Argument(
        help="Target directory path for copy operation.",
        show_default=False,
    ),
    overwrite: bool = typer.Option(
        False,
        help="Overwrites existing files in the target path.",
    ),
    parallel: int = typer.Option(
        4,
        help="Number of parallel threads to use when uploading files.",
    ),
    recursive: bool = typer.Option(
        False,
        help="Copy files recursively with directory structure.",
    ),
    auto_compress: bool = typer.Option(
        default=False,
        help="Specifies whether Snowflake uses gzip to compress files during upload. Ignored when downloading.",
    ),
    refresh: bool = typer.Option(
        default=False,
        help="Specifies whether ALTER STAGE {name} REFRESH should be executed after uploading.",
    ),
    **options,
) -> CommandResult:
    """
    Copies all files from source path to target directory. This works for uploading
    to and downloading files from the stage, and copying between named stages.
    """
    is_get = is_stage_path(source_path)
    is_put = is_stage_path(destination_path)

    if is_get and is_put:
        cursor = StageManager().copy_files(
            source_path=source_path, destination_path=destination_path
        )
        return QueryResult(cursor)
    if not is_get and not is_put:
        raise click.ClickException(
            "Both source and target path are local. This operation is not supported."
        )

    if is_get:
        return get(
            recursive=recursive,
            source_path=source_path,
            destination_path=destination_path,
            parallel=parallel,
        )
    return _put(
        recursive=recursive,
        source_path=Path(source_path),
        destination_path=destination_path,
        parallel=parallel,
        overwrite=overwrite,
        auto_compress=auto_compress,
        refresh=refresh,
    )


@app.command("create", requires_connection=True)
def stage_create(
    stage_name: FQN = StageNameArgument,
    encryption: InternalStageEncryptionType = typer.Option(
        InternalStageEncryptionType.SNOWFLAKE_FULL.value,
        "--encryption",
        help="Type of encryption supported for all files stored on the stage.",
    ),
    enable_directory: bool = typer.Option(
        False,
        "--enable-directory",
        help="Specifies whether directory support is enabled for the stage.",
    ),
    **options,
) -> CommandResult:
    """
    Creates a named stage if it does not already exist.
    """
    cursor = StageManager().create(
        fqn=stage_name, encryption=encryption, enable_directory=enable_directory
    )
    return SingleQueryResult(cursor)


@app.command("remove", requires_connection=True)
def stage_remove(
    stage_name: FQN = StageNameArgument,
    file_name: str = typer.Argument(
        ...,
        help="Name of the file to remove.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """
    Removes a file from a stage.
    """

    cursor = StageManager().remove(stage_name=stage_name.identifier, path=file_name)
    return SingleQueryResult(cursor)


@app.command("diff", hidden=True, requires_connection=True)
def stage_diff(
    stage_name: str = typer.Argument(
        help="Fully qualified name of a stage",
        show_default=False,
    ),
    folder_name: str = typer.Argument(
        help="Path to local folder",
        show_default=False,
    ),
    **options,
) -> Optional[CommandResult]:
    """
    Diffs a stage with a local folder.
    """
    diff: DiffResult = compute_stage_diff(
        local_root=Path(folder_name),
        stage_path=StageManager.stage_path_parts_from_str(stage_name),  # noqa: SLF001
    )
    if get_cli_context().output_format.is_json:
        return ObjectResult(diff.to_dict())
    else:
        print_diff_to_console(diff)
        return None  # don't print any output


@app.command(
    "execute",
    requires_connection=True,
    docs=CommandDocs(
        related=(
            link("/developer-guide/snowflake-cli/index"),
            link(
                "/developer-guide/snowflake-cli/command-reference/overview",
                "Snowflake CLI command reference",
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/stage-commands/overview"
            ),
            link("/developer-guide/snowflake-cli/stages/manage-stages"),
        ),
        usage_notes=(
            PYTHON_EXECUTE_VERSION_SUPPORT,
            plain_text(
                "The command searches for files with a ",
                code(".sql"),
                " extension in the specified ",
                code("STAGE_PATH"),
                " and executes ",
                code("EXECUTE IMMEDIATE"),
                " on each of them. ",
                code("STAGE_PATH"),
                " can be:",
            ),
            bullet_list(
                bullet(
                    "Only a stage name, such as ",
                    code("@scripts"),
                    ", which executes all ",
                    code(".sql"),
                    " files from the stage.",
                ),
                bullet(
                    "Glob-like pattern, such as ",
                    code("@scripts/dir/*"),
                    ", which executes ",
                    code(".sql"),
                    " files from the ",
                    code("dir"),
                    " directory.",
                ),
                bullet(
                    "Direct file path, such as ",
                    code("@scripts/script.sql"),
                    ", which executes only the ",
                    code("script.sql"),
                    " file from the ",
                    code("scripts"),
                    ".",
                ),
            ),
            plain_text(
                "The ",
                code("--silent"),
                " options hides intermediate messages with file execution results.",
            ),
            plain_text(
                "When using Jinja templates for the SQL files, you can pass template "
                "variables using ",
                code("-D"),
                " (or ",
                code("--variable"),
                ") option, such as ",
                code('-D "<key>=<value>"'),
                ". You must enclose string values in single quotes (",
                code("''"),
                ").",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Specify only a stage name to execute all ",
                    code(".sql"),
                    " files in the stage:",
                ),
                command='snow stage execute "@scripts"',
                output=(
                    "SUCCESS - scripts/script1.sql\n"
                    "SUCCESS - scripts/script2.sql\n"
                    "SUCCESS - scripts/dir/script.sql\n"
                    "+------------------------------------------+\n"
                    "| File                   | Status  | Error |\n"
                    "|------------------------+---------+-------|\n"
                    "| scripts/script1.sql    | SUCCESS | None  |\n"
                    "| scripts/script2.sql    | SUCCESS | None  |\n"
                    "| scripts/dir/script.sql | SUCCESS | None  |\n"
                    "+------------------------------------------+"
                ),
            ),
            Example(
                description=plain_text(
                    "Specify a glob-like pattern to execute all ",
                    code(".sql"),
                    " files in the ",
                    code("dir"),
                    " directory:",
                ),
                command='snow stage execute "@scripts/dir/*"',
                output=(
                    "SUCCESS - scripts/dir/script.sql\n"
                    "+------------------------------------------+\n"
                    "| File                   | Status  | Error |\n"
                    "|------------------------+---------+-------|\n"
                    "| scripts/dir/script.sql | SUCCESS | None  |\n"
                    "+------------------------------------------+"
                ),
            ),
            Example(
                description=plain_text(
                    "Specify a glob-like pattern to execute only ",
                    code(".sql"),
                    " files in the ",
                    code("dir"),
                    ' directory that begin with "script", followed by one character:',
                ),
                command='snow stage execute "@scripts/script?.sql"',
                output=(
                    "SUCCESS - scripts/script1.sql\n"
                    "SUCCESS - scripts/script2.sql\n"
                    "+---------------------------------------+\n"
                    "| File                | Status  | Error |\n"
                    "|---------------------+---------+-------|\n"
                    "| scripts/script1.sql | SUCCESS | None  |\n"
                    "| scripts/script2.sql | SUCCESS | None  |\n"
                    "+---------------------------------------+"
                ),
            ),
            Example(
                description=plain_text(
                    "Specify a direct file path with the ", code("--silent"), " option:"
                ),
                command='snow stage execute "@scripts/script1.sql" --silent',
                output=(
                    "+---------------------------------------+\n"
                    "| File                | Status  | Error |\n"
                    "|---------------------+---------+-------|\n"
                    "| scripts/script1.sql | SUCCESS | None  |\n"
                    "+---------------------------------------+"
                ),
            ),
        ),
    ),
)
def execute(
    stage_path: str = typer.Argument(
        ...,
        help="Stage path with files to be execute. For example `@stage/dev/*`.",
        show_default=False,
    ),
    on_error: OnErrorType = OnErrorOption,
    variables: Optional[List[str]] = ExecuteVariablesOption,
    **options,
):
    """
    Execute immediate all files from the stage path. Files can be filtered with a glob-like pattern,
    e.g. `@stage/*.sql`, `@stage/dev/*`. Only files with `.sql` extension will be executed.
    """
    results = StageManager().execute(
        stage_path_str=stage_path, on_error=on_error, variables=variables
    )
    return CollectionResult(results)


def get(recursive: bool, source_path: str, destination_path: str, parallel: int):
    target = Path(destination_path).resolve()
    if not recursive:
        cli_console.warning(
            "Use `--recursive` flag, which copy files recursively with directory structure. This will be the default behavior in the future."
        )
        cursor = StageManager().get(
            stage_path=source_path, dest_path=target, parallel=parallel
        )
        return QueryResult(cursor)

    cursors = StageManager().get_recursive(
        stage_path=source_path, dest_path=target, parallel=parallel
    )
    results = [list(QueryResult(c).result) for c in cursors]
    flattened_results = list(itertools.chain.from_iterable(results))
    sorted_results = sorted(
        flattened_results,
        key=lambda e: (path.dirname(e["file"]), path.basename(e["file"])),
    )
    return CollectionResult(sorted_results)


def _put(
    recursive: bool,
    source_path: Path,
    destination_path: str,
    parallel: int,
    overwrite: bool,
    auto_compress: bool,
    refresh: bool,
):
    if recursive and not source_path.is_file():
        cursor_generator = StageManager().put_recursive(
            local_path=source_path,
            stage_path=destination_path,
            overwrite=overwrite,
            parallel=parallel,
            auto_compress=auto_compress,
        )
        output = CollectionResult(cursor_generator)
    else:
        cursor = StageManager().put(
            local_path=source_path.resolve(),
            stage_path=destination_path,
            overwrite=overwrite,
            parallel=parallel,
            auto_compress=auto_compress,
        )
        output = QueryResult(cursor)

    if refresh:
        StageManager().refresh(
            StageManager.stage_path_parts_from_str(destination_path).stage_name
        )
    return output
