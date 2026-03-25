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

from pathlib import Path
from typing import List, Optional

import typer
from snowflake.cli._plugins.run.manager import ScriptManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import variables_option
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
)
from snowflake.cli.api.sanitizers import sanitize_for_terminal

app = SnowTyperFactory(
    name="run",
    help="Execute project scripts defined in snowflake.yml or manifest.yml.",
)


@app.command(name="run", no_args_is_help=False)
@with_project_definition(is_optional=True)
def run_script(
    script_name: Optional[str] = typer.Argument(
        None,
        help="Name of the script to run.",
    ),
    list_scripts: bool = typer.Option(
        False,
        "--list",
        "-l",
        help="List available scripts.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be executed without running.",
    ),
    continue_on_error: bool = typer.Option(
        False,
        "--continue-on-error",
        help="Continue executing composite scripts even if a step fails.",
    ),
    var_overrides: Optional[List[str]] = variables_option(
        "Override script variables in key=value format.",
    ),
    extra_args: Optional[List[str]] = typer.Argument(
        None,
        help="Additional arguments to pass to the script (use -- before args).",
    ),
    **options,
) -> CommandResult:
    """
    Execute project scripts defined in snowflake.yml or manifest.yml.

    Scripts are defined in the 'scripts' section of your snowflake.yml or manifest.yml file.
    Use --list to see available scripts. Pass additional arguments after --.

    Example usage:

        snow run deploy

        snow run dev -- --server.port 8502

        snow run dcm-plan --var database=PROD

    Variable interpolation supports:

        ${database}, ${schema}, ${connection} - from defaults section

        ${env.VAR_NAME} - from env section or environment variables

        ${entity.<name>.<prop>} - from entity definitions
    """
    ctx = get_cli_context()

    project_root = ctx.project_root
    if project_root is None:
        project_root = Path.cwd()

    manager = ScriptManager(project_root)

    if list_scripts:
        scripts = manager.list_scripts()
        if not scripts:
            return MessageResult("No scripts defined in snowflake.yml or manifest.yml")

        source = manager.scripts_source or "project"
        return CollectionResult(
            [
                {
                    "name": name,
                    "description": script.description or "",
                    "source": source,
                }
                for name, script in scripts.items()
            ]
        )

    if not script_name:
        scripts = manager.list_scripts()
        if not scripts:
            return MessageResult(
                "No scripts defined. Add a 'scripts' section to snowflake.yml or manifest.yml"
            )
        source = manager.scripts_source or "project"
        cc.message(f"Available scripts from {source} (use --list for details):")
        for name in scripts:
            cc.message(f"  {sanitize_for_terminal(name)}")
        return MessageResult("\nSpecify a script name to run, or use --list")

    vars_dict = (
        {v.key: v.value for v in parse_key_value_variables(var_overrides)}
        if var_overrides
        else {}
    )

    script = manager.get_script(script_name)
    if not script:
        available = list(manager.list_scripts().keys())
        if available:
            raise CliError(
                f"Script '{script_name}' not found. Available scripts: {', '.join(available)}"
            )
        raise CliError(
            f"Script '{script_name}' not found. No scripts defined in snowflake.yml or manifest.yml"
        )

    result = manager.execute_script(
        script_name,
        extra_args=extra_args,
        var_overrides=vars_dict,
        dry_run=dry_run,
        verbose=ctx.verbose,
        continue_on_error=continue_on_error,
    )

    if not result.success:
        raise CliError(
            f"Script '{script_name}' failed with exit code {result.exit_code}"
        )
    return MessageResult("")
