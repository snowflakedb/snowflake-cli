from __future__ import annotations

from pathlib import Path
from snowcli.cli.stage.diff import DiffResult

import typer
from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.stage.manager import StageManager
from snowcli.output.decorators import with_output
from snowcli.output.types import (
    ObjectResult,
    QueryResult,
    SingleQueryResult,
    CommandResult,
)

app = typer.Typer(
    name="stage",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages stages.",
)

StageNameOption = typer.Argument(..., help="Name of the stage.")


@app.command("list")
@with_output
@global_options_with_connection
def stage_list(
    stage_name: str = typer.Argument(None, help="Name of the stage."), **options
) -> CommandResult:
    """
    Lists the stage contents or shows available stages if the stage name is omitted.
    """
    manager = StageManager()

    if stage_name:
        cursor = manager.list(stage_name=stage_name)
    else:
        cursor = manager.show()
    return QueryResult(cursor)


@app.command("get")
@with_output
@global_options_with_connection
def stage_get(
    stage_name: str = StageNameOption,
    path: Path = typer.Argument(
        Path.cwd(),
        exists=False,
        file_okay=True,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        help="Directory location to store downloaded files. If omitted, the uploads files in the active directory.",
    ),
    **options,
) -> CommandResult:
    """
    Downloads all files from a stage to a local directory.
    """
    cursor = StageManager().get(stage_name=stage_name, dest_path=path)
    return SingleQueryResult(cursor)


@app.command("put")
@with_output
@global_options_with_connection
def stage_put(
    path: Path = typer.Argument(
        ...,
        exists=False,
        file_okay=True,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        help="File or directory to upload to stage. You can use the `*` wildcard in the path, like `folder/*.csv`. If a path contains `*.`, you must enclose the path in quotes.",
    ),
    stage_name: str = StageNameOption,
    overwrite: bool = typer.Option(
        False,
        help="Overwrites existing files in the stage.",
    ),
    parallel: int = typer.Option(
        4,
        help="Number of parallel threads to use when uploading files. Default: 4.",
    ),
    **options,
) -> CommandResult:
    """
    Uploads files to a stage from a local client.
    """
    manager = StageManager()
    local_path = str(path) + "/*" if path.is_dir() else str(path)

    cursor = manager.put(
        local_path=local_path,
        stage_path=stage_name,
        overwrite=overwrite,
        parallel=parallel,
    )
    return SingleQueryResult(cursor)


@app.command("create")
@with_output
@global_options_with_connection
def stage_create(stage_name: str = StageNameOption, **options) -> CommandResult:
    """
    Creates a named stage if it does not already exist.
    """
    cursor = StageManager().create(stage_name=stage_name)
    return SingleQueryResult(cursor)


@app.command("drop")
@with_output
@global_options_with_connection
def stage_drop(stage_name: str = StageNameOption, **options) -> CommandResult:
    """
    Drops a stage.
    """
    cursor = StageManager().drop(stage_name=stage_name)
    return SingleQueryResult(cursor)


@app.command("remove")
@with_output
@global_options_with_connection
def stage_remove(
    stage_name: str = StageNameOption,
    file_name: str = typer.Argument(..., help="Name of the file to remove."),
    **options,
) -> CommandResult:
    """
    Removes a file from a stage.
    """

    cursor = StageManager().remove(stage_name=stage_name, path=file_name)
    return SingleQueryResult(cursor)


@app.command("diff", hidden=True)
@with_output
@global_options_with_connection
def stage_diff(
    stage_name: str = typer.Argument(None, help="Fully qualified name of a stage"),
    folder_name: str = typer.Argument(None, help="Path to local folder"),
    **options,
) -> ObjectResult:
    """
    Diffs a stage with a local folder.
    """
    diff: DiffResult = stage_diff(Path(folder_name), stage_name)
    return ObjectResult(
        {
            "only on local": diff.only_local,
            "only on stage": diff.only_on_stage,
            "modified/unknown": diff.different,
            "identical": diff.identical,
        }
    )
