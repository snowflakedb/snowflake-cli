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
import os
import sys
from datetime import date, datetime
from decimal import Decimal
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
    preview=True,
)

log = logging.getLogger(__name__)


_PRE_RELEASE_WARNING = (
    "WARNING: 'snow feature' is a pre-release tool. Breaking changes "
    "may occur in future releases. Do not rely on its current API "
    "surface for production workflows."
)
_ANSI_BOLD_RED = "\x1b[1;31m"
_ANSI_RESET = "\x1b[0m"


@app.callback()
def _emit_pre_release_warning() -> None:
    """Emit the red pre-release warning before every `snow feature` invocation.

    Writes directly to ``sys.stderr`` so the banner never corrupts
    structured (JSON / CSV) output that subcommands send to stdout.
    ANSI colour codes are emitted only when stderr is a terminal and
    the user has not opted out via the ``NO_COLOR`` standard env var.
    """
    use_color = sys.stderr.isatty() and os.environ.get("NO_COLOR") is None
    prefix = _ANSI_BOLD_RED if use_color else ""
    suffix = _ANSI_RESET if use_color else ""
    sys.stderr.write(f"{prefix}{_PRE_RELEASE_WARNING}{suffix}\n")
    sys.stderr.flush()


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


# Columns to surface for ``snow feature list`` results.  The Snowflake path
# returns a single multi-kind list (FeatureView, Entity, Datasource), each
# tagged with a ``type`` column.  ``details`` is included so that
# ``--format json`` round-trips the kind-specific extras (e.g.
# ``details.source_type``, ``details.referenced_by``, and — for FeatureView
# rows — ``details.scheduling_state``).  In table mode the ``details`` cell
# renders as a compact dict, which is a deliberate trade-off — operators
# inspecting the table see at-a-glance what the runtime status is, while
# scripts consuming JSON get the full structured data they need.
#
# Datasource rows surface ``details.source_type`` (``Stream`` /
# ``OfflineTable`` / etc.) in the rendered ``type`` column instead of the
# generic ``Datasource`` label, mirroring how FeatureView rows already
# render the specific subkind (``StreamingFeatureView`` etc.).  See
# ``_project_columns`` for the swap.  ``AppliedObject.kind`` is unchanged
# in the underlying data; the swap is display-only.
#
# Two upstream-row fields are intentionally *not* projected:
#
# * ``scheduling_state`` — only meaningful for FeatureView rows (Entity /
#   Datasource leave it empty).  The value is already carried inside
#   ``details`` for FV rows.
# * ``database_name`` / ``schema_name`` — uniform across every row of a
#   single ``snow feature list`` invocation, since the Snowflake
#   connection has one current database and schema.  These are surfaced
#   once above the table by ``_print_listing_scope_header`` instead of
#   being repeated in every row.
_TABLE_DISPLAY_COLUMNS = [
    "type",
    "name",
    "version",
    "entities",
    "created_on",
    "details",
]


def _project_columns(rows: list[dict]) -> list[dict]:
    """Project rows onto ``_TABLE_DISPLAY_COLUMNS`` with stable order.

    Every output row carries **all** display columns in the canonical
    ``_TABLE_DISPLAY_COLUMNS`` order, with ``""`` substituted for any
    field a row does not populate.  This is critical for the table
    renderer, which positions cells by each row's dict iteration
    order: heterogeneous rows (FeatureView vs Entity vs Datasource)
    would otherwise misalign — e.g. the Entity ``type`` value
    landing under the FeatureView-only ``created_on`` column.

    Datasource ``type`` rewrite: when a row carries the canonical
    ``Datasource`` kind and ``details.source_type`` is populated, the
    rendered ``type`` cell is swapped to the specific source type
    (``Stream`` / ``OfflineTable`` / etc.) so operators can
    distinguish stream vs table backings at a glance — mirroring how
    FeatureView rows already render the specific subkind
    (``StreamingFeatureView`` etc.).  This is a display-only
    transformation; ``AppliedObject.kind`` is unchanged in the
    underlying data, so internal grouping (``kind == "Datasource"``)
    continues to work.  When ``source_type`` is missing or empty, the
    fallback is the original ``Datasource`` label.
    """
    if not rows:
        return rows
    out: list[dict] = []
    for row in rows:
        # Case-insensitive lookup of this row's actual keys.
        lower_to_actual = {k.lower(): k for k in row}
        new_row: dict = {}
        for col in _TABLE_DISPLAY_COLUMNS:
            actual = lower_to_actual.get(col.lower())
            value = row.get(actual, "") if actual else ""
            if col == "type" and value == "Datasource":
                details = row.get("details") or {}
                if isinstance(details, dict):
                    source_type = details.get("source_type")
                    if source_type:
                        value = source_type
            new_row[col] = value
        out.append(new_row)
    return out


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
    """Render plan/apply results: ops as a table, or a summary message.

    The overall ``Status: <status>`` header is emitted on stderr by
    ``_print_status_header`` at the call site (so it's present for
    both the table path and the empty-ops path).  This message body
    therefore omits the ``Status:`` line to avoid double-printing —
    it carries only ``Operations: 0`` and any ``Warnings:`` so the
    stdout payload stays compact.
    """
    ops = result.get("ops", [])
    warnings = result.get("warnings", [])
    if ops:
        return _to_collection(ops, all_columns=True)
    parts = ["Operations: 0"]
    if warnings:
        parts.append("Warnings:")
        parts.extend(f"  - {w}" for w in warnings)
    return _to_message("\n".join(parts))


def _print_target_header(result: dict) -> None:
    """Print the target database.schema and warehouse header to stderr."""
    db = result.get("target_database", "")
    schema = result.get("target_schema", "")
    wh = result.get("target_warehouse", "")
    sys.stderr.write(f"\nTarget: {db}.{schema} (warehouse: {wh})\n\n")
    sys.stderr.flush()


def _listing_scope(rows: list[dict]) -> Optional[tuple[str, str]]:
    """Inspect list rows and derive the database / schema scope label.

    Args:
        rows: The result rows from ``FeatureManager.list_specs``.  Each
            row may carry ``database_name`` and ``schema_name`` fields
            (Snowflake-mode listings always do; file-mode listings do
            not).

    Returns:
        A two-tuple ``(database_label, schema_label)`` suitable for
        rendering above the table.  Each label is the uniform value
        when every row agrees, or ``"(multiple)"`` when at least two
        rows disagree on that field.  Returns ``None`` if no row has
        a non-empty ``database_name`` or ``schema_name`` (file-mode
        output, or empty input), signaling that no scope header
        should be printed.
    """
    if not rows:
        return None
    dbs: set[str] = {str(r["database_name"]) for r in rows if r.get("database_name")}
    schemas: set[str] = {str(r["schema_name"]) for r in rows if r.get("schema_name")}
    if not dbs and not schemas:
        return None
    db_label: str = next(iter(dbs)) if len(dbs) == 1 else "(multiple)"
    schema_label: str = next(iter(schemas)) if len(schemas) == 1 else "(multiple)"
    return (db_label, schema_label)


def _print_listing_scope_header(rows: list[dict]) -> None:
    """Write a one-line ``Database: X  Schema: Y`` header to stderr.

    The header is written on stderr (separate from the table on stdout
    or the JSON payload from ``--format json``) so it remains visible
    in both rendering modes without polluting machine-readable output.
    Skipped silently when ``_listing_scope(rows)`` returns ``None``
    (e.g. file-mode listings that have no Snowflake scope).

    Args:
        rows: The result rows from ``FeatureManager.list_specs``.

    Returns:
        None.  The header is emitted to ``sys.stderr`` as a side
        effect.
    """
    scope = _listing_scope(rows)
    if scope is None:
        return
    db, schema = scope
    sys.stderr.write(f"\nDatabase: {db}  Schema: {schema}\n\n")
    sys.stderr.flush()


def _print_mode_header(full_sync: bool) -> None:
    """Print deletion detection mode to stderr."""
    if full_sync:
        sys.stderr.write("Mode: full sync (./...) — orphaned objects will be dropped\n")
    else:
        sys.stderr.write("Mode: incremental — only changes will be applied\n")
    sys.stderr.flush()


def _print_status_header(result: dict) -> None:
    """Print the apply/plan overall status to stderr.

    Mirrors ``_print_mode_header`` / ``_print_target_header`` /
    ``_print_listing_scope_header``: header on stderr, payload on stdout.
    Emitting on stderr means ``--format json`` callers are unaffected
    (the JSON payload on stdout already carries ``status``) while
    operators and shell-script consumers get a single canonical
    ``Status: <status>`` line per invocation regardless of whether the
    payload renders as an ops table (non-empty ``ops``) or as a
    summary message (empty ``ops``).

    Without this helper, ``Status:`` appeared only on the empty-ops
    branch of ``_ops_result`` — which made ``snow feature apply`` look
    silent on a successful CREATE_FV (the table carries per-op status
    cells but no overall header), forcing every script to grep for
    table cells instead of a single stable line.

    The line carries three numbers:

    * total ``Operations`` (table row count),
    * ``executed`` (rows the runtime actually ran — derived from
      ``result["executed"]`` when the manager reports it, otherwise
      counted from ``ops[].status == "success"``).

    A missing or empty ``status`` is silently skipped — the helper is
    a no-op for results that legitimately don't carry one (e.g. raw
    sub-results that bypass ``_ops_result``).
    """
    status = result.get("status", "")
    if not status:
        return
    ops = result.get("ops", []) or []
    total = len(ops)
    executed_value = result.get("executed")
    if executed_value is None:
        executed_value = sum(1 for o in ops if o.get("status") == "success")
    sys.stderr.write(
        f"Status: {status}  Operations: {total} (executed: {executed_value})\n"
    )
    sys.stderr.flush()


def _is_full_sync(input_files: list) -> bool:
    """Return True if *input_files* indicates full-sync mode.

    Full-sync triggers when:

    - any element ends in ``/...`` (or is ``./...``) — explicit recursive
      marker, or
    - any element points to an existing directory on disk — bare directory
      paths are auto-expanded to ``<dir>/...`` by the loader and we mirror
      that here so the CLI mode header and ``no_delete`` flag stay
      consistent with the actual file walk.
    """
    import os as _os

    if not input_files:
        return False
    for f in input_files:
        stripped = f.rstrip("/")
        if stripped.endswith("/...") or f == "./...":
            return True
        if _os.path.isdir(stripped):
            return True
    return False


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def init(
    no_scaffold: bool = typer.Option(
        False, "--no-scaffold", help="Skip local directory creation."
    ),
    **options,
) -> CommandResult:
    """Initialize a feature store in the current database and schema."""
    result = FeatureManager().init(no_scaffold=no_scaffold)
    return _to_object(result)


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
    plan: Optional[str] = typer.Option(
        None,
        "--plan",
        help="Path to a pre-computed plan JSON file (from 'snow feature plan'). "
        "When provided, spec files are not re-loaded and state is not re-queried.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Apply spec files to Snowflake, creating or updating feature-store objects.

    Use './...' as the path to enable full-sync mode: objects deployed in Snowflake
    but not present in local spec files will be dropped. Any other path (specific
    files, directories, or globs) runs in incremental mode — only changes are applied.

    Apply is a pure plan-file consumer: it auto-discovers the latest unapplied
    plan under ``<cwd>/.snowflake/plans/`` (or consumes ``--plan <path>``).  Use
    ``snow feature plan`` to preview changes before applying.
    """
    if plan is None and not input_files:
        raise typer.BadParameter(
            "At least one file is required (or --plan <path>).",
            param_hint="INPUT_FILES",
        )
    full_sync = _is_full_sync(input_files or [])
    if plan is None:
        _print_mode_header(full_sync)
    result = FeatureManager().apply(
        input_files=input_files or [],
        config=config,
        dev_mode=dev,
        overwrite=overwrite,
        allow_recreate=allow_recreate,
        plan_file=plan,
        no_delete=not full_sync,
    )
    _print_status_header(result)
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
    out: Optional[str] = typer.Option(
        None,
        "--out",
        help="Path to write the plan JSON file. Defaults to "
        ".snowflake/plans/feature_plan_<timestamp>.json.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Show what would change if the spec files were applied (dry-run of apply).

    Use './...' as the path to enable full-sync mode (deletion detection).
    The plan is also written to a JSON file so it can be applied later with
    'snow feature apply --plan <path>'.
    """
    if not input_files:
        raise typer.BadParameter(
            "At least one file is required.", param_hint="INPUT_FILES"
        )

    import os

    full_sync = _is_full_sync(input_files or [])
    _print_mode_header(full_sync)
    no_delete = not full_sync

    from datetime import datetime as _dt

    if out is None:
        ts = _dt.now().strftime("%Y%m%dT%H%M%S")
        out = os.path.join(".snowflake", "plans", f"feature_plan_{ts}.json")

    # Validate (dry-run apply) BEFORE writing the plan file so a failed
    # plan never leaves a stale ``feature_plan_*.json`` on disk.  This
    # mirrors the order ``snow feature apply`` already uses and matches
    # the validate-before-emit invariant called out in the architecture
    # docs (Step 6 — Validation).
    manager = FeatureManager()
    result = manager.apply(
        input_files=input_files,
        config=config,
        dry_run=True,
        dev_mode=dev,
        overwrite=False,
        allow_recreate=False,
        no_delete=no_delete,
    )
    if result.get("status") == "validation_failed":
        _print_status_header(result)
        return _to_object(result)

    # Validation passed → persist the plan to disk.
    plan_path = manager.write_plan(
        input_files=input_files,
        config=config,
        dev_mode=dev,
        out_path=out,
        no_delete=no_delete,
    )
    result["plan_file"] = plan_path
    _print_status_header(result)
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
        _print_listing_scope_header(specs)
        return _to_collection(specs)
    return _to_object(result)


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def describe(
    name: str = typer.Argument(
        ...,
        help="Feature view name (e.g. 'user_event_features'). "
        "Also accepts the full OFT name (NAME$VERSION$ONLINE).",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Describe a single feature-store object."""
    result = FeatureManager().describe(name=name)

    # Print rich display to stderr (not captured by CLI result rendering)
    display = result.pop("_display", None)
    if display:
        import sys

        sys.stderr.write(display + "\n")
        sys.stderr.flush()

    # Remove internal keys from JSON output
    result.pop("examples", None)

    rows = result.get("rows", [])
    if isinstance(rows, list) and rows:
        return _to_collection(rows)
    return _to_object(result)


# ---------------------------------------------------------------------------
# drop
# ---------------------------------------------------------------------------


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
        mgr = FeatureManager()

        # Check if already running before doing anything
        pre_status = mgr.get_status()
        if pre_status.get("status") == "RUNNING":
            return _to_object(
                {
                    "status": "RUNNING",
                    "message": "Service already initialized",
                }
            )

        import itertools
        import sys
        import threading
        import time

        stop_event = threading.Event()
        # Shared state for spinner to display current stage
        stage_info = {"status": "CREATING", "message": "Sending create request..."}

        def _spin():
            """Background thread: animate spinner on stderr with current stage."""
            chars = itertools.cycle(["|", "/", "-", "\\"])
            start = time.monotonic()
            while not stop_event.is_set():
                elapsed = int(time.monotonic() - start)
                c = next(chars)
                st = stage_info.get("status", "")
                msg = stage_info.get("message", "")
                line = f"\r  {c} [{elapsed}s] {st}"
                if msg:
                    line += f": {msg}"
                # Pad to overwrite previous longer lines
                line = line.ljust(80)
                sys.stderr.write(line)
                sys.stderr.flush()
                stop_event.wait(0.5)

        # Start spinner immediately
        spinner_thread = threading.Thread(target=_spin, daemon=True)
        spinner_thread.start()

        # Send CREATE on main thread (has Click context)
        result = mgr.initialize_service(
            producer_role=producer_role,
            consumer_role=consumer_role,
        )

        if result.get("status") == "error":
            stop_event.set()
            spinner_thread.join(timeout=2)
            sys.stderr.write("\r" + " " * 80 + "\r")
            sys.stderr.flush()
            return _to_object(result)

        if result.get("status") == "RUNNING":
            stop_event.set()
            spinner_thread.join(timeout=2)
            sys.stderr.write("\r  Online service is RUNNING." + " " * 52 + "\n")
            sys.stderr.flush()
            return _to_object(result)

        # Poll on main thread until RUNNING or timeout
        stage_info["message"] = "Waiting for service to start..."
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            time.sleep(5)
            try:
                status = mgr.get_status()
                current = status.get("status", "unknown")
                message = status.get("message", "")
                stage_info["status"] = current
                stage_info["message"] = message
                if current == "RUNNING":
                    stop_event.set()
                    spinner_thread.join(timeout=2)
                    sys.stderr.write("\r  Online service is RUNNING." + " " * 52 + "\n")
                    sys.stderr.flush()
                    return _to_object(
                        {
                            "status": "RUNNING",
                            "message": "Service initialized successfully",
                        }
                    )
            except Exception:
                pass

        stop_event.set()
        spinner_thread.join(timeout=2)
        sys.stderr.write("\r  Timed out waiting for RUNNING." + " " * 48 + "\n")
        sys.stderr.flush()
        return _to_object({"status": "timeout", "error": "Timed out after 600s"})
    elif drop:
        result = FeatureManager().destroy_service()
    else:
        # Status mode: show rich formatted display
        result = FeatureManager().get_status()
        if result.get("status") != "error":
            import sys

            from snowflake.ml.feature_store.decl import api as decl_api

            display = decl_api.format_status_display(
                result,
                user=result.pop("_user", ""),
                database=result.pop("_database", ""),
                schema=result.pop("_schema", ""),
            )
            sys.stderr.write(display + "\n")
            sys.stderr.flush()
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
    version: str = typer.Option(
        ...,
        "--version",
        help=(
            "Feature view version (e.g. 'V1').  Required because "
            "snowml-core's online lookup is keyed on (name, version) — "
            "there is no 'latest' fallback for a bare name."
        ),
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
            feature_view_name=feature_view_name,
            version=version,
            keys=parsed_keys,
        )
    except RuntimeError as exc:
        raise ClickException(str(exc))
    return _to_object(result)
