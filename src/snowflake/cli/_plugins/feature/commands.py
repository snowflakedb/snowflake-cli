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
import sys
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional

import typer
from click import ClickException
from snowflake.cli._plugins.feature.manager import FeatureManager
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    ObjectResult,
)

app = SnowTyperFactory(
    name="feature",
    help="Manages declarative feature-store objects in Snowflake.",
)

log = logging.getLogger(__name__)


def _safe_value(o):
    """Coerce non-serializable values to strings for display."""
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, bytes):
        return o.decode("utf-8", errors="replace")
    return o


def _sanitize_dict(d: dict) -> dict:
    """Make a dict safe for table/JSON rendering."""
    return {k: _safe_value(v) for k, v in d.items()}


def _to_object(data: dict) -> CommandResult:
    """Single-object result — renders as key-value table."""
    return ObjectResult(_sanitize_dict(data))


# Columns to show in table output for SHOW ONLINE FEATURE TABLES results.
# All columns are still available via --format json.
_TABLE_DISPLAY_COLUMNS = [
    "feature_view",
    "version",
    "entities",
    "created_on",
    "scheduling_state",
]


def _project_columns(rows: list[dict]) -> list[dict]:
    """Keep only the display columns (case-insensitive match) for table output."""
    if not rows:
        return rows
    # Build a case-insensitive lookup from the first row's actual keys.
    first = rows[0]
    keep = set()
    lower_to_actual = {k.lower(): k for k in first}
    for col in _TABLE_DISPLAY_COLUMNS:
        actual = lower_to_actual.get(col.lower())
        if actual:
            keep.add(actual)
    if not keep:
        return rows  # none matched — return everything
    return [{k: v for k, v in r.items() if k in keep} for r in rows]


def _to_collection(rows: list[dict], *, all_columns: bool = False) -> CommandResult:
    """Multi-row result — renders as a table with column headers.

    By default, only the display columns are shown. Pass all_columns=True
    to include everything (used for non-Snowflake results).
    """
    sanitized = [_sanitize_dict(r) for r in rows]
    if not all_columns:
        sanitized = _project_columns(sanitized)
    return CollectionResult(sanitized)


def _to_message(text: str) -> CommandResult:
    """Plain text message."""
    return MessageResult(text)


def _ops_result(result: dict) -> CommandResult:
    """Render plan/apply results: ops as a table, or a summary message."""
    ops = result.get("ops", [])
    warnings = result.get("warnings", [])
    status = result.get("status", "")
    if ops:
        return _to_collection(ops, all_columns=True)
    parts = [f"Status: {status}", f"Operations: 0"]
    if warnings:
        parts.append("Warnings:")
        parts.extend(f"  - {w}" for w in warnings)
    return _to_message("\n".join(parts))


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
    if result.get("status") == "validation_failed":
        return _to_object(result)
    return _ops_result(result)


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
    if result.get("status") == "validation_failed":
        return _to_object(result)
    return _ops_result(result)


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
    specs = result.get("specs", [])
    if isinstance(specs, list) and specs and isinstance(specs[0], dict):
        return _to_collection(specs)
    return _to_object(result)


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
    rows = result.get("rows", [])
    if isinstance(rows, list) and rows:
        return _to_collection(rows)
    return _to_object(result)


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
    return _to_object(result)


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
    return _to_object(result)


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
    result = FeatureManager().generate_example(output_dir or ".")
    files = result.get("files", [])
    if files:
        return _to_collection([{"file": f} for f in files], all_columns=True)
    return _to_object(result)


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def export(
    output_dir: Optional[str] = typer.Option(
        None,
        "--dir",
        help="Base output directory. Defaults to current directory.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Export deployed feature-store objects from Snowflake as YAML spec files."""
    result = FeatureManager().export_specs(output_dir or ".")
    files = result.get("files", [])
    return _to_collection([{"file": f} for f in files], all_columns=True)


# ---------------------------------------------------------------------------
# online-service
# ---------------------------------------------------------------------------


@app.command(name="online-service", requires_connection=True)
def online_service(
    create: bool = typer.Option(
        False, "--create", help="Create and initialize the online service."
    ),
    drop: bool = typer.Option(
        False,
        "--drop",
        help="Destroy the online service and all Online Feature Tables.",
    ),
    producer_role: Optional[str] = typer.Option(
        None,
        "--producer-role",
        help="Role for producing features. Defaults to the connection role.",
        show_default=False,
    ),
    consumer_role: Optional[str] = typer.Option(
        None,
        "--consumer-role",
        help="Role for consuming features. Defaults to PUBLIC.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Manage the feature store online service. Shows status by default."""
    if create and drop:
        raise typer.BadParameter(
            "Cannot use --create and --drop together.", param_hint="--create/--drop"
        )
    if create:
        result = FeatureManager().initialize_service(
            producer_role=producer_role,
            consumer_role=consumer_role,
        )
    elif drop:
        result = FeatureManager().destroy_service()
    else:
        result = FeatureManager().get_status()
    return _to_object(result)


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def ingest(
    source_name: str = typer.Argument(
        ...,
        help="Name of the streaming source to ingest records into.",
        show_default=False,
    ),
    data: str = typer.Option(
        "-",
        "--data",
        help="Path to a JSON file containing a records array, or - to read from stdin.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Ingest records into a streaming feature source via the Online Service."""
    if data == "-":
        content = sys.stdin.read()
    else:
        try:
            with open(data) as fh:
                content = fh.read()
        except OSError as exc:
            raise typer.BadParameter(str(exc), param_hint="--data")

    try:
        records = json.loads(content)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"Invalid JSON: {exc}", param_hint="--data")

    try:
        result = FeatureManager().ingest(source_name=source_name, records=records)
    except RuntimeError as exc:
        raise ClickException(str(exc))
    return _to_object(result)


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def query(
    feature_view_name: str = typer.Argument(
        ...,
        help="Name of the feature view to query.",
        show_default=False,
    ),
    keys: str = typer.Option(
        ...,
        "--keys",
        help='JSON array of entity key objects, e.g. \'[{"user_id": "u1"}]\'.',
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Query online features for a feature view via the Online Service."""
    try:
        parsed_keys = json.loads(keys)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"Invalid JSON: {exc}", param_hint="--keys")

    try:
        result = FeatureManager().query(
            feature_view_name=feature_view_name, keys=parsed_keys
        )
    except RuntimeError as exc:
        raise ClickException(str(exc))
    return _to_object(result)
