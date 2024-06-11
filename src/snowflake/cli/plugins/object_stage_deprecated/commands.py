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

# TODO 3.0: remove these commands

from __future__ import annotations

import typer
from snowflake.cli.api.commands.flags import (
    PatternOption,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.plugins.command import CommandPath
from snowflake.cli.plugins.stage.commands import (
    StageNameArgument,
    copy,
    stage_create,
    stage_diff,
    stage_list_files,
    stage_remove,
)

_deprecated_command_msg = (
    f"`{CommandPath(['object', 'stage'])}` command group is deprecated."
    f" Please use `{CommandPath(['stage'])}` instead."
)

app = SnowTyperFactory(name="stage", help="Manages stages.", deprecated=True)


@app.callback()
def warn_command_deprecated() -> None:
    cli_console.warning(_deprecated_command_msg)


@app.command("list", requires_connection=True, deprecated=True)
def deprecated_stage_list(
    stage_name: str = StageNameArgument, pattern=PatternOption, **options
):
    """
    Lists the stage contents.
    """
    return stage_list_files(stage_name=stage_name, pattern=pattern, **options)


@app.command("copy", requires_connection=True, deprecated=True)
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
    """
    Copies all files from target path to target directory. This works for both uploading
    to and downloading files from the stage.
    """
    copy(
        source_path=source_path,
        destination_path=destination_path,
        overwrite=overwrite,
        parallel=parallel,
        recursive=recursive,
    )


@app.command("create", requires_connection=True, deprecated=True)
def deprecated_stage_create(stage_name: str = StageNameArgument, **options):
    """
    Creates a named stage if it does not already exist.
    """
    stage_create(stage_name=stage_name, **options)


@app.command("remove", requires_connection=True, deprecated=True)
def deprecated_stage_remove(
    stage_name: str = StageNameArgument,
    file_name: str = typer.Argument(..., help="Name of the file to remove."),
    **options,
):
    """
    Removes a file from a stage.
    """
    stage_remove(stage_name=stage_name, file_name=file_name)


@app.command("diff", hidden=True, requires_connection=True, deprecated=True)
def deprecated_stage_diff(
    stage_name: str = typer.Argument(None, help="Fully qualified name of a stage"),
    folder_name: str = typer.Argument(None, help="Path to local folder"),
    **options,
):
    """
    Diffs a stage with a local folder.
    """
    return stage_diff(stage_name=stage_name, folder_name=folder_name, **options)
