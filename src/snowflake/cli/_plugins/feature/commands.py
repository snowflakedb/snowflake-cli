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

"""Typer commands for 'snow feature'."""

from __future__ import annotations

import json
import logging
from enum import Enum
from typing import List, Optional

import typer
from snowflake.cli._plugins.feature.manager import FeatureManager, generate_example
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult

app = SnowTyperFactory(
    name="feature",
    help="Manages declarative feature-store objects in Snowflake.",
)

log = logging.getLogger(__name__)


def _to_result(data: dict) -> CommandResult:
    """Format a manager result dict as a CLI MessageResult."""
    return MessageResult(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def apply(
    input_files: Optional[List[str]] = typer.Argument(
        None,
        help="Spec file paths or glob patterns to apply.",
        show_default=False,
    ),
    dry: bool = typer.Option(False, "--dry", help="Show plan without executing."),
    dev: bool = typer.Option(
        False, "--dev", help="Apply in dev mode (relaxed validation)."
    ),
    overwrite: bool = typer.Option(
        False, "--overwrite", help="Overwrite existing objects."
    ),
    allow_recreate: bool = typer.Option(
        False, "--allow-recreate", help="Allow destructive recreation of objects."
    ),
    config: Optional[str] = typer.Option(
        None, "--config", help="Path to Jinja2 config file.", show_default=False
    ),
    **options,
) -> CommandResult:
    """Apply spec files to Snowflake, creating or updating feature-store objects."""
    if not input_files:
        raise typer.BadParameter(
            "At least one file is required.", param_hint="INPUT_FILES"
        )
    result = FeatureManager().apply(
        input_files=input_files,
        config=config,
        dry_run=dry,
        dev_mode=dev,
        overwrite=overwrite,
        allow_recreate=allow_recreate,
    )
    return _to_result(result)


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def plan(
    input_files: Optional[List[str]] = typer.Argument(
        None,
        help="Spec file paths or glob patterns to plan.",
        show_default=False,
    ),
    dev: bool = typer.Option(
        False, "--dev", help="Plan in dev mode (relaxed validation)."
    ),
    config: Optional[str] = typer.Option(
        None, "--config", help="Path to Jinja2 config file.", show_default=False
    ),
    **options,
) -> CommandResult:
    """Show what would change if the spec files were applied (dry-run of apply)."""
    if not input_files:
        raise typer.BadParameter(
            "At least one file is required.", param_hint="INPUT_FILES"
        )
    result = FeatureManager().apply(
        input_files=input_files,
        config=config,
        dry_run=True,
        dev_mode=dev,
        overwrite=False,
        allow_recreate=False,
    )
    return _to_result(result)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@app.command(name="list", requires_connection=True)
def list_cmd(
    input_files: Optional[List[str]] = typer.Argument(
        None,
        help="Optional spec files; omit to list deployed objects from Snowflake.",
        show_default=False,
    ),
    config: Optional[str] = typer.Option(
        None, "--config", help="Path to Jinja2 config file.", show_default=False
    ),
    **options,
) -> CommandResult:
    """List feature-store specs from files or deployed objects from Snowflake."""
    result = FeatureManager().list_specs(
        input_files=tuple(input_files) if input_files else (),
        config=config,
    )
    return _to_result(result)


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def describe(
    name: str = typer.Argument(
        ...,
        help="Name of the feature-store object to describe. "
        "Use --database/--schema connection flags to specify location.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Describe a single feature-store object."""
    result = FeatureManager().describe(name=name)
    return _to_result(result)


# ---------------------------------------------------------------------------
# drop
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def drop(
    names: Optional[List[str]] = typer.Argument(
        None,
        help="Names of feature-store objects to drop. "
        "Use --database/--schema connection flags to specify location.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Drop one or more feature-store objects."""
    if not names:
        raise typer.BadParameter("At least one name is required.", param_hint="NAMES")
    result = FeatureManager().drop(names=names)
    return _to_result(result)


# ---------------------------------------------------------------------------
# convert
# ---------------------------------------------------------------------------


class ConvertFormat(str, Enum):
    yaml = "yaml"
    json = "json"


@app.command(requires_connection=True)
def convert(
    input_files: Optional[List[str]] = typer.Argument(
        None,
        help="Spec file paths (Python DSL) to convert.",
        show_default=False,
    ),
    file_format: ConvertFormat = typer.Option(
        ...,
        "--file-format",
        help="Target file format: yaml or json.",
        show_default=False,
    ),
    out_dir: Optional[str] = typer.Option(
        None,
        "--out-dir",
        help="Output directory for converted files.",
        show_default=False,
    ),
    recursive: bool = typer.Option(
        False,
        "-R",
        help="Retain relative directory structure in output.",
    ),
    config: Optional[str] = typer.Option(
        None, "--config", help="Path to Jinja2 config file.", show_default=False
    ),
    **options,
) -> CommandResult:
    """Convert Python DSL spec files to YAML or JSON format."""
    if not input_files:
        raise typer.BadParameter(
            "At least one file is required.", param_hint="INPUT_FILES"
        )
    result = FeatureManager().convert(
        input_files=input_files,
        file_format=file_format.value,
        output_dir=out_dir,
        recursive=recursive,
        config=config,
    )
    return _to_result(result)


# ---------------------------------------------------------------------------
# example
# ---------------------------------------------------------------------------


@app.command()
def example(
    output_dir: Optional[str] = typer.Option(
        None,
        "--dir",
        help="Directory to write example files into. Defaults to the current directory.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Generate example YAML spec files for testing (no Snowflake connection required)."""
    result = generate_example(output_dir or ".")
    return _to_result(result)
