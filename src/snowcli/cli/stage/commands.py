from __future__ import annotations

from pathlib import Path

import typer
from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.stage.manager import StageManager
from snowcli.output.decorators import with_output
from snowcli.output.types import QueryResult, SingleQueryResult, CommandResult

app = typer.Typer(
    name="stage",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage stages",
)

StageNameOption = typer.Argument(..., help="Stage name.")


@app.command("list")
@with_output
@global_options_with_connection
def stage_list(
    stage_name: str = typer.Argument(None, help="Name of stage"), **options
) -> CommandResult:
    """
    List stage contents or shows available stages if stage name not provided.
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
        help="Directory location to store downloaded files",
    ),
    **options,
) -> CommandResult:
    """
    Download all files from a stage to a local directory.
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
        help="File or directory to upload to stage, can include a `*` in the path, like `folder/*.csv`. Make sure you put quotes around the path if it includes a `*`. ",
    ),
    name: str = StageNameOption,
    overwrite: bool = typer.Option(
        False,
        help="Overwrite existing files in stage",
    ),
    parallel: int = typer.Option(
        4,
        help="Number of parallel threads to use for upload",
    ),
    **options,
) -> CommandResult:
    """
    Upload files to a stage from a local client
    """
    manager = StageManager()
    local_path = str(path) + "/*" if path.is_dir() else str(path)

    cursor = manager.put(
        local_path=local_path, stage_path=name, overwrite=overwrite, parallel=parallel
    )
    return SingleQueryResult(cursor)


@app.command("create")
@with_output
@global_options_with_connection
def stage_create(name: str = StageNameOption, **options) -> CommandResult:
    """
    Create stage if not exists.
    """
    cursor = StageManager().create(stage_name=name)
    return SingleQueryResult(cursor)


@app.command("drop")
@with_output
@global_options_with_connection
def stage_drop(name: str = StageNameOption, **options) -> CommandResult:
    """
    Drop stage
    """
    cursor = StageManager().drop(stage_name=name)
    return SingleQueryResult(cursor)


@app.command("remove")
@with_output
@global_options_with_connection
def stage_remove(
    stage_name: str = StageNameOption,
    file_name: str = typer.Argument(..., help="File name"),
    **options,
) -> CommandResult:
    """
    Remove file from stage
    """

    cursor = StageManager().remove(stage_name=stage_name, path=file_name)
    return SingleQueryResult(cursor)
