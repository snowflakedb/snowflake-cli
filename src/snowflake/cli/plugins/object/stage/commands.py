from __future__ import annotations

from pathlib import Path

import click
import typer
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import (
    CommandResult,
    ObjectResult,
    QueryResult,
    SingleQueryResult,
)
from snowflake.cli.plugins.object.stage.diff import DiffResult
from snowflake.cli.plugins.object.stage.manager import StageManager

app = SnowTyper(
    name="stage",
    help="Manages stages.",
)

StageNameArgument = typer.Argument(..., help="Name of the stage.")


@app.command("list", requires_connection=True)
def stage_list(stage_name: str = StageNameArgument, **options) -> CommandResult:
    """
    Lists the stage contents.
    """
    cursor = StageManager().list_files(stage_name=stage_name)
    return QueryResult(cursor)


def _is_stage_path(path: str):
    return path.startswith("@") or path.startswith("snow://")


@app.command("copy", requires_connection=True)
def copy(
    source_path: str = typer.Argument(
        help="Source path for copy operation. Can be either stage path or local."
    ),
    destination_path: str = typer.Argument(
        help="Target directory path for copy operation. Should be stage if source is local or local if source is stage.",
    ),
    overwrite: bool = typer.Option(
        False,
        help="Overwrites existing files in the target path.",
    ),
    parallel: int = typer.Option(
        4,
        help="Number of parallel threads to use when uploading files.",
    ),
    **options,
) -> CommandResult:
    """
    Copies all files from target path to target directory. This works for both uploading
    to and downloading files from the stage.
    """
    is_get = _is_stage_path(source_path)
    is_put = _is_stage_path(destination_path)

    if is_get and is_put:
        raise click.ClickException(
            "Both source and target path are remote. This operation is not supported."
        )
    if not is_get and not is_put:
        raise click.ClickException(
            "Both source and target path are local. This operation is not supported."
        )

    if is_get:
        target = Path(destination_path).resolve()
        cursor = StageManager().get(
            stage_name=source_path, dest_path=target, parallel=parallel
        )
    else:
        source = Path(source_path).resolve()
        local_path = str(source) + "/*" if source.is_dir() else str(source)

        cursor = StageManager().put(
            local_path=local_path,
            stage_path=destination_path,
            overwrite=overwrite,
            parallel=parallel,
        )
    return QueryResult(cursor)


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
    stage_name: str = typer.Argument(None, help="Fully qualified name of a stage"),
    folder_name: str = typer.Argument(None, help="Path to local folder"),
    **options,
) -> ObjectResult:
    """
    Diffs a stage with a local folder.
    """
    diff: DiffResult = stage_diff(Path(folder_name), stage_name)
    return ObjectResult(str(diff))
