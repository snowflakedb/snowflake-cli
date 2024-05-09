from __future__ import annotations

import itertools
from os import path
from pathlib import Path
from typing import List, Optional

import click
import typer
from snowflake.cli.api.commands.flags import (
    OnErrorOption,
    PatternOption,
    VariablesOption,
)
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    ObjectResult,
    QueryResult,
    SingleQueryResult,
)
from snowflake.cli.api.utils.path_utils import is_stage_path
from snowflake.cli.plugins.stage.diff import DiffResult, compute_stage_diff
from snowflake.cli.plugins.stage.manager import OnErrorType, StageManager

app = SnowTyper(
    name="stage",
    help="Manages stages.",
)

StageNameArgument = typer.Argument(..., help="Name of the stage.", show_default=False)


@app.command("list-files", requires_connection=True)
def stage_list_files(
    stage_name: str = StageNameArgument, pattern=PatternOption, **options
) -> CommandResult:
    """
    Lists the stage contents.
    """
    cursor = StageManager().list_files(stage_name=stage_name, pattern=pattern)
    return QueryResult(cursor)


@app.command("copy", requires_connection=True)
def copy(
    source_path: str = typer.Argument(
        help="Source path for copy operation. Can be either stage path or local.",
        show_default=False,
    ),
    destination_path: str = typer.Argument(
        help="Target directory path for copy operation. Should be stage if source is local or local if source is stage.",
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
    **options,
) -> CommandResult:
    """
    Copies all files from target path to target directory. This works for both uploading
    to and downloading files from the stage.
    """
    is_get = is_stage_path(source_path)
    is_put = is_stage_path(destination_path)

    if is_get and is_put:
        raise click.ClickException(
            "Both source and target path are remote. This operation is not supported."
        )
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
        source_path=source_path,
        destination_path=destination_path,
        parallel=parallel,
        overwrite=overwrite,
    )


@app.command("create", requires_connection=True)
def stage_create(stage_name: str = StageNameArgument, **options) -> CommandResult:
    """
    Creates a named stage if it does not already exist.
    """
    cursor = StageManager().create(stage_name=stage_name)
    return SingleQueryResult(cursor)


@app.command("remove", requires_connection=True)
def stage_remove(
    stage_name: str = StageNameArgument,
    file_name: str = typer.Argument(..., help="Name of the file to remove."),
    **options,
) -> CommandResult:
    """
    Removes a file from a stage.
    """

    cursor = StageManager().remove(stage_name=stage_name, path=file_name)
    return SingleQueryResult(cursor)


@app.command("diff", hidden=True, requires_connection=True)
def stage_diff(
    stage_name: str = typer.Argument(help="Fully qualified name of a stage"),
    folder_name: str = typer.Argument(help="Path to local folder"),
    **options,
) -> ObjectResult:
    """
    Diffs a stage with a local folder.
    """
    diff: DiffResult = compute_stage_diff(Path(folder_name), stage_name)
    return ObjectResult(str(diff))


@app.command("execute", requires_connection=True)
def execute(
    stage_path: str = typer.Argument(
        ...,
        help="Stage path with files to be execute. For example `@stage/dev/*`.",
        show_default=False,
    ),
    on_error: OnErrorType = OnErrorOption,
    variables: Optional[List[str]] = VariablesOption,
    **options,
):
    """
    Execute immediate all files from the stage path. Files can be filtered with glob like pattern,
    e.g. `@stage/*.sql`, `@stage/dev/*`. Only files with `.sql` extension will be executed.
    """
    results = StageManager().execute(
        stage_path=stage_path, on_error=on_error, variables=variables
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
    source_path: str,
    destination_path: str,
    parallel: int,
    overwrite: bool,
):
    if recursive:
        raise click.ClickException("Recursive flag for upload is not supported.")

    source = Path(source_path).resolve()
    local_path = str(source) + "/*" if source.is_dir() else str(source)

    cursor = StageManager().put(
        local_path=local_path,
        stage_path=destination_path,
        overwrite=overwrite,
        parallel=parallel,
    )
    return QueryResult(cursor)
