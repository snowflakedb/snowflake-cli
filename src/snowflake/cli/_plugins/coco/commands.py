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

import typer
from snowflake.cli._plugins.coco.cortex_code import (
    _get_install_dir,
    run_cortex_code,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliError

app = SnowTyperFactory()


@app.command(
    name="coco",
    requires_connection=False,
    requires_global_options=False,
    context_settings={
        "allow_extra_args": True,
        "allow_interspersed_args": False,
        "ignore_unknown_options": True,
    },
    short_help="Runs the Cortex Code CLI.",
)
def coco(
    ctx: typer.Context,
    help_flag: bool = typer.Option(
        False,
        "--help",
        "-h",
        help="Show this message and exit.",
        is_eager=True,
    ),
    remove: bool = typer.Option(
        False,
        "--remove",
        help="Remove the downloaded Cortex Code CLI",
        is_eager=True,
    ),
):
    """
    Runs the Cortex Code CLI.

    If already installed, `snow` will execute the Cortex Code CLI found in your PATH.

    If the Cortex Code CLI is not installed, it will be downloaded to {install_dir}.

    Use `--remove` to remove the downloaded Cortex Code CLI.

    This command is only supported on Windows, Linux, and Darwin, on amd64/x64
    or arm64 architectures.

    To prevent `snow` from interpreting flags intended for Cortex Code,
    use `--` before Cortex Code flags and args.

    Examples:

        # Run the Cortex Code CLI

        $ snow coco --

        # Run the Cortex Code CLI with arguments

        $ snow coco -- --help

        # Remove the Cortex Code CLI (if installed through snow)

        $ snow coco --remove
    """.format(
        install_dir=_get_install_dir()
    )
    args = ctx.args
    has_separator = "--" in sys.argv

    if help_flag or (not args and not remove and not has_separator):
        cli_console.message(ctx.get_help())
        raise SystemExit(0)

    if remove and args:
        raise CliError("Cannot use --remove with args")

    exit_code = run_cortex_code(args, remove=remove)
    sys.exit(exit_code)
