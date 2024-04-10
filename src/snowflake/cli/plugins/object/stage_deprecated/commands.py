# TODO 3.0: remove these commands

from __future__ import annotations

from typing import List, Optional

import typer
from snowflake.cli.api.commands.flags import (
    OnErrorOption,
    PatternOption,
    VariablesOption,
)
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.plugins.command import CommandPath
from snowflake.cli.plugins.stage.commands import (
    StageNameArgument,
    copy,
    execute,
    stage_create,
    stage_diff,
    stage_list_files,
    stage_remove,
)
from snowflake.cli.plugins.stage.manager import OnErrorType

_deprecated_command_msg = (
    f"`{CommandPath(['object', 'stage'])}` command group is deprecated."
    f" Please use `{CommandPath(['stage'])}` instead."
)

app = SnowTyper(name="stage", help=_deprecated_command_msg)


@app.callback()
def warn_command_deprecated() -> None:
    cli_console.warning(_deprecated_command_msg)


@app.command("list", requires_connection=True)
def deprecated_stage_list(
    stage_name: str = StageNameArgument, pattern=PatternOption, **options
):
    """This command is deprecated. Please use `snow stage list-files` instead."""
    return stage_list_files(stage_name=stage_name, pattern=pattern, **options)


@app.command("copy", requires_connection=True)
def deprecated_copy(
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
    recursive: bool = typer.Option(
        False,
        help="Copy files recursively with directory structure.",
    ),
    **options,
):
    """This command is deprecated. Please use `snow stage copy` instead."""
    copy(
        source_path=source_path,
        destination_path=destination_path,
        overwrite=overwrite,
        parallel=parallel,
        recursive=recursive,
    )


@app.command("create", requires_connection=True)
def deprecated_stage_create(stage_name: str = StageNameArgument, **options):
    """This command is deprecated. Please use `snow stage create` instead."""
    stage_create(stage_name=stage_name, **options)


@app.command("remove", requires_connection=True)
def deprecated_stage_remove(
    stage_name: str = StageNameArgument,
    file_name: str = typer.Argument(..., help="Name of the file to remove."),
    **options,
):
    """This command is deprecated. Please use `snow stage remove` instead."""
    stage_remove(stage_name=stage_name, file_name=file_name)


@app.command("diff", hidden=True, requires_connection=True)
def deprecated_stage_diff(
    stage_name: str = typer.Argument(None, help="Fully qualified name of a stage"),
    folder_name: str = typer.Argument(None, help="Path to local folder"),
    **options,
):
    """This command is deprecated. Please use `snow stage diff` instead."""
    return stage_diff(stage_name=stage_name, folder_name=folder_name, **options)


@app.command("execute", requires_connection=True)
def deprecated_execute(
    stage_path: str = typer.Argument(
        ...,
        help="Stage path with files to be execute. For example `@stage/dev/*`.",
        show_default=False,
    ),
    on_error: OnErrorType = OnErrorOption,
    variables: Optional[List[str]] = VariablesOption,
    **options,
):
    """This command is deprecated. Please use `snow stage execute` instead."""
    execute(stage_path=stage_path, on_error=on_error, variables=variables, **options)
