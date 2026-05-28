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

"""Typer commands for 'snow feature' (manifest-driven, Phase 3+4).

The CLI surface mirrors DCM (D3): every command takes a
``--from <dir>`` flag (default cwd) and a ``--target <name>`` flag
(default = the manifest's ``default_target``).  ``--variable
key=value`` is the only template-variable mechanism (D5: the legacy
``--config`` flag is removed).  Apply consumes plan files only —
neither ``apply`` nor ``plan`` accepts positional spec paths or the
``./...`` recursive marker (D1).
"""

from __future__ import annotations

import functools
import json
import logging
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, List, Optional, TypeVar

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

# ``FeatureStoreNotInitializedError`` is the declarative-layer wrapper
# raised by :func:`decl_api.assert_feature_store_initialized` when a
# command runs against a schema that has not been bootstrapped via
# ``snow feature init``.  Imported at the module top so the wrapping
# decorator (:func:`_surface_init_required_as_click_exception`) can
# catch it for every command — see Phase 6 of
# ``plans/remove_duplicate_entity_tag_prefix_ec78aade.plan.md``.
try:
    from snowflake.ml.feature_store.decl.errors import (
        FeatureStoreNotInitializedError,
    )

    _HAS_FS_NOT_INIT_ERROR = True
except ImportError:  # pragma: no cover — wheel-isolation fallback

    class FeatureStoreNotInitializedError(Exception):  # type: ignore[no-redef]
        """Fallback stub when the snowml decl wheel is too old to ship
        :class:`FeatureStoreNotInitializedError`.

        Older decl wheels never raise this exception, so the
        ``except`` clause in
        :func:`_surface_init_required_as_click_exception` is effectively
        a no-op.  The stub exists only so the import does not fail
        on those wheels.
        """

    _HAS_FS_NOT_INIT_ERROR = False


_F = TypeVar("_F", bound=Callable[..., Any])


def _surface_init_required_as_click_exception(fn: _F) -> _F:
    """Wrap a Typer command so :class:`FeatureStoreNotInitializedError`
    becomes a top-level :class:`ClickException`.

    Pin NT6 from the plan: every ``snow feature`` subcommand except
    ``init`` must convert the snowml-layer init-required error into
    an actionable CLI error.  The message produced by
    ``FeatureStoreNotInitializedError.__str__`` already contains the
    operator-facing remediation
    (``"Run \\`snow feature init\\` against this target to bootstrap"``),
    so we propagate it verbatim — Click's default handler renders the
    message on stderr and exits with code 1.

    Args:
        fn: The Typer command function to wrap.

    Returns:
        The wrapped function — behaviourally identical except that
        :class:`FeatureStoreNotInitializedError` is caught and
        re-raised as :class:`ClickException`.
    """

    @functools.wraps(fn)
    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except FeatureStoreNotInitializedError as exc:
            raise ClickException(str(exc)) from exc

    return _wrapper  # type: ignore[return-value]


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
    """
    use_color = sys.stderr.isatty() and os.environ.get("NO_COLOR") is None
    prefix = _ANSI_BOLD_RED if use_color else ""
    suffix = _ANSI_RESET if use_color else ""
    sys.stderr.write(f"{prefix}{_PRE_RELEASE_WARNING}{suffix}\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------
# Shared options (DCM-strict surface)
# ---------------------------------------------------------------------------


def _from_option_callback(value: Optional[Path]) -> Path:
    """Default ``--from`` to the current working directory."""
    if value is None:
        return Path.cwd()
    return Path(value)


from_option = typer.Option(
    None,
    "--from",
    help="Local directory containing the feature-store project (must "
    "contain manifest.yml). Omit to use the current directory.",
    show_default=False,
    callback=_from_option_callback,
)


target_option = typer.Option(
    None,
    "--target",
    help="Target profile from manifest.yml to use. Uses default_target "
    "when not specified.",
    show_default=False,
)


variables_option = typer.Option(
    None,
    "--variable",
    "-D",
    help="Variables for the project's templating context, e.g. "
    '`-D "<key>=<value>"`. May be repeated.',
    show_default=False,
)


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


_TABLE_DISPLAY_COLUMNS = [
    "type",
    "name",
    "version",
    "entities",
    "created_on",
    "details",
]


def _project_columns(rows: list[dict]) -> list[dict]:
    """Project rows onto ``_TABLE_DISPLAY_COLUMNS`` with stable order."""
    if not rows:
        return rows
    out: list[dict] = []
    for row in rows:
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
    """Multi-row result — renders as a table with column headers."""
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
    if ops:
        return _to_collection(ops, all_columns=True)
    parts = ["Operations: 0"]
    if warnings:
        parts.append("Warnings:")
        parts.extend(f"  - {w}" for w in warnings)
    return _to_message("\n".join(parts))


def _print_target_header(result: dict) -> None:
    """Print the resolved manifest target + warehouse to stderr."""
    db = result.get("target_database", "")
    schema = result.get("target_schema", "")
    wh = result.get("target_warehouse", "")
    name = result.get("target_name", "")
    if name:
        sys.stderr.write(f"\nTarget: {name} @ {db}.{schema} (warehouse: {wh})\n\n")
    else:
        sys.stderr.write(f"\nTarget: {db}.{schema} (warehouse: {wh})\n\n")
    sys.stderr.flush()


def _listing_scope(rows: list[dict]) -> Optional[tuple[str, str]]:
    """Inspect list rows and derive the database / schema scope label."""
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
    """Write a one-line ``Database: X  Schema: Y`` header to stderr."""
    scope = _listing_scope(rows)
    if scope is None:
        return
    db, schema = scope
    sys.stderr.write(f"\nDatabase: {db}  Schema: {schema}\n\n")
    sys.stderr.flush()


def _print_status_header(result: dict) -> None:
    """Print the apply/plan overall status to stderr.

    Header on stderr, payload on stdout: scripts and JSON-mode
    callers read the structured payload from stdout, while operators
    see the canonical ``Status: <status>  Operations: N (executed: M)``
    line on stderr regardless of how the payload renders.
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


# ---------------------------------------------------------------------------
# init — single bootstrap command (subsumes the deleted `export` command)
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
def init(
    target: Optional[str] = typer.Option(
        None,
        "--target",
        help=(
            "Manifest target name.  On a brand-new manifest this names "
            "the only target (default 'DEFAULT').  On a re-init, picks "
            "which existing manifest target to export from "
            "(default = manifest's default_target)."
        ),
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Bootstrap a feature-store project and pull deployed artifacts.

    Always runs in the current directory.  On a fresh init, writes
    ``manifest.yml`` derived from the active connection (use the
    global ``--database`` / ``--schema`` flags to point the new
    target at a different schema than the connection's default),
    scaffolds ``sources/{entities,datasources,feature_views}/`` plus
    ``out/plan/.gitkeep``, runs the Snowflake-side ``FeatureStore``
    bootstrap (CREATE_IF_NOT_EXIST), and pulls every deployed object
    into ``sources/`` as YAML.

    Re-running ``init`` is fully idempotent: the existing manifest is
    preserved, the FS bootstrap re-runs, and the export refreshes the
    on-disk artifacts so they stay in sync with the deployed runtime.
    On a re-init, ``--database`` / ``--schema`` values that differ
    from the resolved manifest target are rejected with a top-level
    error (the manifest is the source of truth — edit it directly to
    move a target).
    """
    # ``--database`` / ``--schema`` arrive in ``**options`` via the
    # global ``requires_connection`` decorator.  Forward both values
    # to the manager so a fresh init can bake them into the new
    # manifest, and a re-init can detect a mismatch against the
    # resolved manifest target.  Without this forwarding the override
    # was silently dropped (the original bug — the manifest ended up
    # carrying the connection profile's default schema).
    db_override: Optional[str] = options.get("database")
    sch_override: Optional[str] = options.get("schema")
    del options
    result = FeatureManager().init(
        project_root=Path.cwd(),
        target_name=target,
        database=db_override,
        schema=sch_override,
    )
    return _to_object(result)


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@_surface_init_required_as_click_exception
def apply(
    from_location: Path = from_option,
    target: Optional[str] = target_option,
    variables: Optional[List[str]] = variables_option,
    dev: bool = typer.Option(
        False, "--dev", help="Apply in dev mode (relaxed validation)."
    ),
    allow_recreate: bool = typer.Option(
        False, "--allow-recreate", help="Allow destructive recreation of objects."
    ),
    plan: Optional[str] = typer.Option(
        None,
        "--plan",
        help="Path to a pre-computed plan JSON file (from 'snow feature plan'). "
        "When provided, skips the auto-discovery of out/plan/.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Apply the discovered (or explicit) plan against Snowflake.

    Apply is a *pure plan-file consumer*: it auto-discovers the
    latest unapplied plan under ``<project_root>/out/plan/`` (or
    consumes ``--plan <path>``).  Use ``snow feature plan`` to
    preview changes and produce a plan file first.
    """
    # ``--variable`` is forwarded to apply only so the surface stays
    # uniform across plan / apply (Acceptance #8).  Apply itself does
    # not currently consume runtime variables — the plan envelope it
    # consumes is already fully resolved — but we keep the flag so
    # operators can use the same invocation form for both commands
    # without surprises.
    del variables

    result = FeatureManager().apply(
        from_dir=from_location,
        target_name=target,
        plan_file=plan,
        dev_mode=dev,
        allow_recreate=allow_recreate,
    )
    _print_target_header(result)
    _print_status_header(result)
    return _ops_result(result)


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@_surface_init_required_as_click_exception
def plan(
    from_location: Path = from_option,
    target: Optional[str] = target_option,
    variables: Optional[List[str]] = variables_option,
    dev: bool = typer.Option(
        False, "--dev", help="Plan in dev mode (relaxed validation)."
    ),
    out: Optional[str] = typer.Option(
        None,
        "--out",
        help="Path to write the plan JSON file. Defaults to "
        "<project_root>/out/plan/feature_plan_<timestamp>.json.",
        show_default=False,
    ),
    no_delete: bool = typer.Option(
        False,
        "--no-delete",
        help="Disable deletion detection.  By default, ``plan`` runs "
        "in full-sync mode against the manifest project.",
    ),
    **options,
) -> CommandResult:
    """Show what would change if the project were applied (read-only).

    Plans run against the manifest project: ``--from <dir>`` locates
    ``manifest.yml`` and ``--target <name>`` selects the target.  The
    plan is also written to a JSON file under
    ``<project_root>/out/plan/`` so it can be applied later with
    ``snow feature apply``.
    """
    manager = FeatureManager()
    result = manager.plan(
        from_dir=from_location,
        target_name=target,
        variables=variables,
        dev_mode=dev,
        allow_recreate=False,
        no_delete=no_delete,
    )
    _print_target_header(result)
    if result.get("status") == "validation_failed":
        _print_status_header(result)
        return _to_object(result)

    plan_path = manager.write_plan(
        from_dir=from_location,
        target_name=target,
        variables=variables,
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
@_surface_init_required_as_click_exception
def list_cmd(
    from_location: Path = from_option,
    target: Optional[str] = target_option,
    variables: Optional[List[str]] = variables_option,
    **options,
) -> CommandResult:
    """List deployed feature-store objects from Snowflake."""
    del variables  # accepted for surface uniformity; not consumed here.
    result = FeatureManager().list_specs(from_dir=from_location, target_name=target)
    specs = result.get("specs", [])
    if isinstance(specs, list) and specs and isinstance(specs[0], dict):
        _print_listing_scope_header(specs)
        return _to_collection(specs)
    return _to_object(result)


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@_surface_init_required_as_click_exception
def describe(
    name: str = typer.Argument(
        ...,
        help="Feature view name (e.g. 'user_event_features'). "
        "Also accepts the full OFT name (NAME$VERSION$ONLINE).",
        show_default=False,
    ),
    from_location: Path = from_option,
    target: Optional[str] = target_option,
    variables: Optional[List[str]] = variables_option,
    **options,
) -> CommandResult:
    """Describe a single feature-store object."""
    del variables
    result = FeatureManager().describe(
        from_dir=from_location, target_name=target, name=name
    )

    display = result.pop("_display", None)
    if display:
        sys.stderr.write(display + "\n")
        sys.stderr.flush()

    result.pop("examples", None)

    rows = result.get("rows", [])
    if isinstance(rows, list) and rows:
        return _to_collection(rows)
    return _to_object(result)


# ---------------------------------------------------------------------------
# online-service
# ---------------------------------------------------------------------------


@app.command(name="online-service", requires_connection=True)
def online_service(
    from_location: Path = from_option,
    target: Optional[str] = target_option,
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
        help="Role for producing features. Defaults to the manifest "
        "target's role (or the connection role if neither is set).",
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
    """Manage the feature store online service. Shows status by default.

    The online service is bound to a specific ``<DB>.<SCHEMA>``
    location, so different manifest targets can run independent
    services in different states.  ``--from`` / ``--target`` route
    every sub-action (status, ``--create``, ``--drop``) through the
    same manifest resolver every other ``snow feature`` command uses.

    When no ``manifest.yml`` is reachable from ``--from`` AND
    ``--target`` is omitted, the command falls back to the active
    connection's database / schema so operators can still query or
    destroy a runtime before scaffolding a project tree.  An
    explicit ``--target`` against a directory without a manifest is
    a hard error (mismatched intent).
    """
    if create and drop:
        raise typer.BadParameter(
            "Cannot use --create and --drop together.", param_hint="--create/--drop"
        )
    if create:
        mgr = FeatureManager()
        pre_status = mgr.get_status(from_dir=from_location, target_name=target)
        if pre_status.get("status") == "RUNNING":
            return _to_object(
                {
                    "status": "RUNNING",
                    "message": "Service already initialized",
                }
            )

        import itertools
        import threading
        import time

        stop_event = threading.Event()
        stage_info = {"status": "CREATING", "message": "Sending create request..."}

        def _spin():
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
                line = line.ljust(80)
                sys.stderr.write(line)
                sys.stderr.flush()
                stop_event.wait(0.5)

        spinner_thread = threading.Thread(target=_spin, daemon=True)
        spinner_thread.start()

        result = mgr.initialize_service(
            from_dir=from_location,
            target_name=target,
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

        stage_info["message"] = "Waiting for service to start..."
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            time.sleep(5)
            try:
                status = mgr.get_status(from_dir=from_location, target_name=target)
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
        result = FeatureManager().destroy_service(
            from_dir=from_location, target_name=target
        )
    else:
        result = FeatureManager().get_status(from_dir=from_location, target_name=target)
        if result.get("status") != "error":
            from snowflake.ml.feature_store.decl import api as decl_api

            display = decl_api.format_status_display(
                result,
                user=result.pop("_user", ""),
                database=result.pop("_database", ""),
                schema=result.pop("_schema", ""),
            )
            sys.stderr.write(display + "\n")
            sys.stderr.flush()
            # The rich display on stderr is the canonical surface for
            # operators; rendering ``result`` as an ObjectResult here
            # would duplicate the same fields on stdout as a key/value
            # table.  Return an empty message instead so the success
            # path stays single-source.
            return _to_message("")
    return _to_object(result)


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@_surface_init_required_as_click_exception
def ingest(
    source_name: str = typer.Argument(
        ...,
        help="Name of the streaming source to ingest records into.",
        show_default=False,
    ),
    from_location: Path = from_option,
    target: Optional[str] = target_option,
    variables: Optional[List[str]] = variables_option,
    data: str = typer.Option(
        "-",
        "--data",
        help="Path to a JSON file containing a records array, or - to read from stdin.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Ingest records into a streaming feature source via the Online Service."""
    del variables
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
        result = FeatureManager().ingest(
            from_dir=from_location,
            target_name=target,
            source_name=source_name,
            records=records,
        )
    except (RuntimeError, ValueError) as exc:
        raise ClickException(str(exc))
    return _to_object(result)


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@_surface_init_required_as_click_exception
def query(
    feature_view_name: str = typer.Argument(
        ...,
        help="Name of the feature view to query.",
        show_default=False,
    ),
    from_location: Path = from_option,
    target: Optional[str] = target_option,
    variables: Optional[List[str]] = variables_option,
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
    del variables
    try:
        parsed_keys = json.loads(keys)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"Invalid JSON: {exc}", param_hint="--keys")

    try:
        result = FeatureManager().query(
            from_dir=from_location,
            target_name=target,
            feature_view_name=feature_view_name,
            version=version,
            keys=parsed_keys,
        )
    except RuntimeError as exc:
        raise ClickException(str(exc))
    return _to_object(result)
