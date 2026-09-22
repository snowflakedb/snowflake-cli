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

"""Typer commands for 'snow feature' (manifest-driven). See DESIGN.md.

Commands take ``--from <dir>`` (default cwd) and ``--target <name>``
(default = manifest ``default_target``); ``--variable key=value`` is the
only template-variable mechanism, and it lives on ``plan`` alone.
Templating is resolved at plan time and baked into the plan JSON, so
``apply`` / ``list`` / ``describe`` / ``ingest`` / ``query`` do not accept
it (they never load or render local specs).
"""

from __future__ import annotations

import functools
import json
import logging
import os
import shutil
import sys
import textwrap
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterator, List, Optional, TypeVar

import typer

# Optional ``snowflake-ml-python[feature_store]`` import. Availability is owned
# by ``manager`` (the module that binds ``decl_api``, which every command path
# dereferences): ``manager.snowml_import_error()`` is the single source of truth
# and ``_require_snowflake_ml`` guards off it. A schema that has not been
# bootstrapped via ``snow feature init`` surfaces as the library's
# ``FeatureStoreNotInitializedError``, resolved lazily off ``manager.decl_api``
# (re-exported from ``decl.api``) so the CLI imports the feature-store library
# through exactly one entry point.
from snowflake.cli._plugins.feature import manager as _feature_manager
from snowflake.cli._plugins.feature.manager import FeatureManager
from snowflake.cli.api.commands.flags import (
    ForceOption,
    InteractiveOption,
    LocalDirectoryType,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import CliArgumentError, CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    EmptyResult,
    MessageResult,
    ObjectResult,
)
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath

_MISSING_SNOWML_MESSAGE = (
    "Error: 'snow feature' requires the snowflake-ml-python[feature_store] library"
)


def _require_snowflake_ml() -> None:
    """Fail with an actionable error when the feature-store library is missing."""
    if _feature_manager.snowml_import_error() is not None:
        raise CliError(_MISSING_SNOWML_MESSAGE)


def _init_required_error() -> type[BaseException]:
    """Resolve the library's init-required exception, or a dead sentinel.

    Uses ``manager.decl_api`` (re-exports ``FeatureStoreNotInitializedError``)
    so the CLI never imports an internal ``decl`` submodule directly. When the
    library is absent, returns ``Exception`` as an unreachable fallback:
    ``feature_store_preflight`` fails first, so no decorated command body runs.
    """
    decl_api = _feature_manager.decl_api
    if decl_api is None:
        return Exception
    return decl_api.FeatureStoreNotInitializedError


_F = TypeVar("_F", bound=Callable[..., Any])


def _surface_init_required_as_cli_error(fn: _F) -> _F:
    """Re-raise the library's init-required error as a ``CliError``.

    Its message already carries the ``snow feature init`` remediation.
    """

    @functools.wraps(fn)
    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except _init_required_error() as exc:
            raise CliError(_term(str(exc))) from exc

    return _wrapper  # type: ignore[return-value]


def feature_store_preflight(fn: _F) -> _F:
    """Guard the library dependency and emit the preview banner.

    Applied per command (not the group callback) so ``snow feature <cmd>
    --help`` works without the library installed — ``--help`` is eager and
    short-circuits before this wrapper runs.
    """

    @functools.wraps(fn)
    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        _require_snowflake_ml()
        _emit_preview_warning()
        return fn(*args, **kwargs)

    return _wrapper  # type: ignore[return-value]


app = SnowTyperFactory(
    name="feature",
    help="Manages declarative feature-store objects in Snowflake.",
    preview=True,
    is_hidden=FeatureFlag.ENABLE_FEATURE_STORE.is_disabled,
)

log = logging.getLogger(__name__)


def _emit_preview_warning() -> None:
    """Emit the yellow public-preview warning on stderr (never on structured stdout)."""
    preview_warning = "WARNING: 'snow feature' is in public preview."
    ansi_yellow = "\x1b[33m"
    ansi_reset = "\x1b[0m"
    use_color = sys.stderr.isatty() and os.environ.get("NO_COLOR") is None
    prefix = ansi_yellow if use_color else ""
    suffix = ansi_reset if use_color else ""
    sys.stderr.write(f"{prefix}{preview_warning}{suffix}\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------
# Shared options (DCM-strict surface)
# ---------------------------------------------------------------------------


def _from_option_callback(value: Optional[SecurePath]) -> SecurePath:
    """Default ``--from`` to the current working directory (same as DCM)."""
    return value if value is not None else SecurePath.cwd()


from_option = typer.Option(
    None,
    "--from",
    help="Local directory containing the feature-store project (must "
    "contain manifest.yml). Omit to use the current directory.",
    show_default=False,
    click_type=LocalDirectoryType(),
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


def _term(value: Any) -> str:
    """Strip ANSI / control sequences from a value bound for the terminal.

    Server- and config-sourced strings (object names, statuses, error text)
    may carry escape sequences; per ``conventions.md`` they must be run
    through ``sanitize_for_terminal`` before printing. None-safe (like DCM's
    ``sanitize_for_terminal(...) or ""``) so callers can format unconditionally.
    """
    if value is None:
        return ""
    text = value if isinstance(value, str) else str(value)
    return sanitize_for_terminal(text) or ""


def _safe_value(o):
    """Coerce non-serializable values to JSON/table-safe form (recursively).

    Findings (``ValidationResult``) become plain dicts so machine callers see
    ``{severity, code, message, object_name}`` instead of a pydantic repr.
    """
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, bytes):
        return o.decode("utf-8", errors="replace")
    if isinstance(o, str):
        # Sanitize here so ops / list collection tables (names, operations,
        # statuses) are stripped even on ``printing.py``'s multi-row path,
        # which does not sanitize. JSON re-strips in ``CustomJSONEncoder``
        # (idempotent).
        return _term(o)
    if isinstance(o, dict):
        return {k: _safe_value(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_safe_value(v) for v in o]
    model_dump = getattr(o, "model_dump", None)
    if callable(model_dump):
        return _safe_value(model_dump())
    parsed = _parse_validation_finding(o)
    if parsed is not None:
        obj, code, msg = parsed
        return {
            "severity": getattr(o, "severity", ""),
            "code": code,
            "message": msg,
            "object_name": obj,
        }
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


# Column order for the ``plan`` / ``apply`` ops table.  ``type`` leads
# (BatchFeatureView / StreamingFeatureView / Entity / FeatureGroup / ...)
# so the ops table matches the ``type``-first layout of ``snow feature
# list``; ``status`` only appears on apply rows.
_OPS_DISPLAY_COLUMNS = [
    "type",
    "name",
    "version",
    "operation",
    "reason",
    "destructive",
    "status",
]


def _project_ops_columns(rows: list[dict]) -> list[dict]:
    """Order op rows by ``_OPS_DISPLAY_COLUMNS``; extra keys follow so no detail is dropped."""
    out: list[dict] = []
    for row in rows:
        ordered: dict = {col: row[col] for col in _OPS_DISPLAY_COLUMNS if col in row}
        for key, value in row.items():
            if key not in ordered:
                ordered[key] = value
        out.append(ordered)
    return out


def _actionable_ops(ops: Optional[list]) -> list:
    """Drop ``NO_CHANGE`` ops for display only; the persisted plan JSON keeps them."""
    return [o for o in (ops or []) if o.get("operation") != "NO_CHANGE"]


def _ops_result(result: dict) -> CommandResult:
    """Render plan/apply results: ops as a table, or a summary message."""
    ops = _project_ops_columns(_actionable_ops(result.get("ops", [])))
    warnings = result.get("warnings", [])
    if ops:
        return _to_collection(ops, all_columns=True)
    parts = ["Operations: 0"]
    if warnings:
        parts.append("Warnings:")
        parts.extend(f"  - {w}" for w in warnings)
    return _to_message("\n".join(parts))


def _print_warnings(result: dict) -> None:
    """Print a result envelope's ``warnings`` to stderr.

    Used by ``init`` / ``sync``, whose object envelopes have no ops table
    to carry them; kept off the machine-readable stdout payload.
    """
    warnings = result.get("warnings") or []
    if not warnings:
        return
    sys.stderr.write("\nWarnings:\n")
    for w in warnings:
        sys.stderr.write(f"  - {_term(w)}\n")
    sys.stderr.write("\n")
    sys.stderr.flush()


def _parse_validation_finding(item: Any) -> Optional[tuple[str, str, str]]:
    """Return ``(object_name, code, message)`` for a finding, else ``None``.

    Accepts a ``ValidationResult`` or a mapping with ``code`` / ``message``;
    returns ``None`` for anything else (e.g. a plain ``init`` / ``sync``
    warning string) so callers print it raw.
    """
    if isinstance(item, Mapping):
        code = item.get("code")
        message = item.get("message")
        obj = item.get("object_name", "")
    else:
        code = getattr(item, "code", None)
        message = getattr(item, "message", None)
        obj = getattr(item, "object_name", "")
    if not isinstance(code, str) or not isinstance(message, str):
        return None
    return (obj or "", code, message)


def _write_finding_block(label: str, items: list) -> None:
    """Write one ``Errors``/``Warnings`` diagnostic block to stderr.

    Findings render as ``OBJECT  [CODE]`` with the message wrapped beneath
    (readable instead of overflowing a table cell); non-findings fall back
    to a plain ``- <text>`` bullet.
    """
    sys.stderr.write(f"\n{label} ({len(items)}):\n")
    width = max(40, shutil.get_terminal_size((80, 24)).columns)
    for item in items:
        parsed = _parse_validation_finding(item)
        if parsed is None:
            sys.stderr.write(f"  - {_term(item)}\n")
            continue
        obj, code, msg = parsed
        obj, code, msg = _term(obj), _term(code), _term(msg)
        head = f"  {obj}  [{code}]" if obj else f"  [{code}]"
        sys.stderr.write(head + "\n")
        if msg:
            sys.stderr.write(
                textwrap.fill(
                    msg,
                    width=width,
                    initial_indent="    ",
                    subsequent_indent="    ",
                )
                + "\n"
            )


def _print_diagnostics(result: dict) -> None:
    """Print a result envelope's ``errors`` / ``warnings`` to stderr.

    Human-facing counterpart to the structured arrays JSON / CSV callers
    read from stdout; renders one finding per line rather than a TABLE cell.
    """
    errors = result.get("errors") or []
    warnings = result.get("warnings") or []
    if not errors and not warnings:
        return
    if errors:
        _write_finding_block("Errors", errors)
    if warnings:
        _write_finding_block("Warnings", warnings)
    sys.stderr.write("\n")
    sys.stderr.flush()


def _compact_failure_envelope(result: dict) -> dict:
    """Drop the verbose ``errors`` / ``warnings`` / ``ops`` cells for TABLE.

    Findings are surfaced on stderr by ``_print_diagnostics``; here they
    become ``error_count`` / ``warning_count`` scalars so the key-value
    TABLE stays readable. JSON / CSV callers keep the untouched envelope.
    """
    compact = {
        k: v for k, v in result.items() if k not in ("errors", "warnings", "ops")
    }
    errors = result.get("errors") or []
    warnings = result.get("warnings") or []
    if errors:
        compact["error_count"] = len(errors)
    if warnings:
        compact["warning_count"] = len(warnings)
    return compact


# Terminal-failure statuses: a populated ``errors`` array that must reach the
# caller AND flip the process exit code non-zero (the way ``snow dcm`` does),
# rather than the historical exit-0 that let ``--format json`` collapse to
# ``{"message": "Operations: 0"}`` or a bare ops array.  ``no_plan`` is
# deliberately excluded — it is an idle *success* (see ``apply``).  ``failed``
# is the ``online-service drop`` total-teardown failure (nothing dropped,
# every drop errored); ``partial_failure`` covers both a partial ``apply`` and
# an ``online-service drop`` that dropped some OFTs but still errored.
_TERMINAL_FAILURE_STATUSES = frozenset(
    {
        "target_mismatch",
        "partial_failure",
        "refused",
        "validation_failed",
        "error",
        "failed",
        # online-service create wait outcomes (uppercase runtime tokens +
        # deadline): each must exit non-zero rather than silently exit 0.
        "timeout",
        "FAILED",
        "SUSPENDED",
        "ERROR",
    }
)


def _is_terminal_failure(result: dict) -> bool:
    """True when ``result['status']`` is a terminal failure (non-zero exit)."""
    return result.get("status") in _TERMINAL_FAILURE_STATUSES


def _envelope_error_messages(result: dict) -> list:
    """Flatten an envelope's ``errors`` (or singular ``error``) to text lines.

    ``ValidationResult``-shaped findings become ``OBJECT [CODE] message``;
    plain strings pass through. Used only to build the human ``CliError``
    summary — the machine-readable arrays stay on the structured payload.
    """
    raw = result.get("errors")
    if not raw:
        single = result.get("error")
        raw = [single] if single else []
    messages: list = []
    for item in raw:
        parsed = _parse_validation_finding(item)
        if parsed is None:
            messages.append(_term(item))
        else:
            obj, code, msg = parsed
            obj, code, msg = _term(obj), _term(code), _term(msg)
            head = f"{obj} [{code}]" if obj else f"[{code}]"
            messages.append(f"{head} {msg}".strip())
    return messages


def _terminal_failure_error(result: dict) -> CliError:
    """Build the ``CliError`` for a terminal-failure envelope (non-zero exit).

    The structured payload already carries ``status`` + ``errors`` on stdout;
    this human summary rides on the ``CliError`` (stderr) so the failure is
    both machine-readable and self-explaining.
    """
    status = _term(result.get("status", "error"))
    messages = _envelope_error_messages(result)
    summary = f"Status: {status}"
    if messages:
        summary += "\n" + "\n".join(str(m) for m in messages)
    return CliError(summary)


def _emit(cmd_result: CommandResult) -> None:
    """Render *cmd_result* immediately (before a terminal ``CliError`` raise).

    Commands normally hand their return value to the Typer output layer; a
    terminal failure raises instead, so the structured envelope has to be
    printed explicitly here first.
    """
    from snowflake.cli._app.printing import print_result

    print_result(cmd_result)


def _fail_terminal(result: dict) -> None:
    """Emit a terminal-failure envelope for the active format, then raise.

    Structured (JSON/CSV): the full envelope (``status`` + ``errors`` /
    ``error``, plus ``ops`` when present) is printed as the stdout payload so
    machine callers still receive the populated arrays. TABLE mode relies on
    the caller having already written human diagnostics to stderr plus the
    ``CliError`` summary. Always raises; never returns.
    """
    if _is_structured_output():
        _emit(_to_object(result))
    raise _terminal_failure_error(result)


def _is_structured_output() -> bool:
    """True when the active ``--format`` is JSON / CSV (machine-readable).

    An unresolvable CLI context is treated as structured so a JSON / CSV
    consumer never receives TABLE text on stdout. The exception is logged
    at DEBUG.
    """
    try:
        from snowflake.cli.api.cli_global_context import get_cli_context
        from snowflake.cli.api.output.formats import OutputFormat

        fmt = get_cli_context().output_format
    except Exception as exc:
        log.debug("could not resolve CLI output format: %s", exc, exc_info=True)
        return True
    return bool(getattr(fmt, "is_json", False)) or fmt == OutputFormat.CSV


def _write_display(text: Optional[str]) -> None:
    """Write a free-form display block to stderr — TABLE mode only.

    Gated on :func:`_is_structured_output` so banners never leak into
    JSON / CSV stdout; a falsy *text* is a no-op.
    """
    if not text or _is_structured_output():
        return
    sys.stderr.write(_term(text) + "\n")
    sys.stderr.flush()


class _NullServiceWaitProgress:
    """No-op wait-progress handle for structured / silent output."""

    def update(self, status: str, message: str = "") -> None:
        return None


class _RichServiceWaitProgress:
    """Relabel a single indeterminate Rich task during the startup wait."""

    def __init__(self, progress: Any, task_id: Any) -> None:
        self._progress = progress
        self._task_id = task_id

    def update(self, status: str, message: str = "") -> None:
        status, message = _term(status), _term(message)
        description = f"{status}: {message}" if message else status
        self._progress.update(self._task_id, description=description)


@contextmanager
def _service_wait_progress(status: str, message: str = "") -> Iterator[Any]:
    """Yield a progress handle for the online-service startup wait.

    Silent / structured output yields a no-op handle; otherwise a transient
    Rich bar on ``sys.stderr``. The wait is an indeterminate poll-until-
    ``RUNNING`` (``total=None``), so the bar pulses and tracks the state.
    """
    from snowflake.cli.api.cli_global_context import get_cli_context

    if get_cli_context().silent:
        yield _NullServiceWaitProgress()
        return

    from rich.console import Console
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )

    initial = f"{status}: {message}" if message else status
    console = Console(file=sys.stderr)
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as progress:
        task_id = progress.add_task(initial, total=None)
        yield _RichServiceWaitProgress(progress, task_id)


# ---------------------------------------------------------------------------
# online-service create startup wait
# ---------------------------------------------------------------------------

# The create wait budget and cadence (unchanged: 5s poll, 600s deadline). The
# consecutive-failure cap bounds how long a *repeatedly failing* poll is
# tolerated before we abort with an error rather than burning the whole budget
# down to a generic timeout. A single transient blip that then resolves to
# RUNNING still succeeds.
_ONLINE_SERVICE_TIMEOUT_S = 600
_ONLINE_SERVICE_POLL_INTERVAL_S = 5
_SERVICE_WAIT_MAX_CONSECUTIVE_FAILURES = 5
_SERVICE_WAIT_SUCCESS_MESSAGE = "Service initialized successfully"

# Terminal runtime statuses reported by SYSTEM$..._STATUS (via
# ``parse_service_status``) that mean the create will not become RUNNING on its
# own. ``ERROR`` here is the uppercase runtime token; the lowercase ``error``
# envelope is the manager's own query-failure signal and is treated as a
# (transient) poll failure, not an immediate runtime-terminal state.
_SERVICE_WAIT_FAILED_STATUSES = frozenset({"FAILED", "SUSPENDED", "ERROR"})


def _wait_online_service_running(
    poll_status: Callable[[], dict],
    progress: Any,
    *,
    timeout_sec: float = _ONLINE_SERVICE_TIMEOUT_S,
    interval_sec: float = _ONLINE_SERVICE_POLL_INTERVAL_S,
    max_consecutive_failures: int = _SERVICE_WAIT_MAX_CONSECUTIVE_FAILURES,
    sleep: Optional[Callable[[float], Any]] = None,
    monotonic: Optional[Callable[[], float]] = None,
) -> dict:
    """Poll ``poll_status`` until the online service reaches a terminal state.

    Returns a status envelope for the caller to render / fail on:

    * ``RUNNING`` — success.
    * ``FAILED`` / ``SUSPENDED`` / ``ERROR`` — the runtime reported a terminal
      failure; returned verbatim so the operator sees the real state instead
      of a generic timeout.
    * ``error`` — the consecutive-failure cap was hit (repeated raised polls or
      manager query-failure envelopes). The swallowed detail is logged at
      ``DEBUG`` and surfaced as the envelope ``error``.
    * ``timeout`` — the deadline elapsed while still in-flight.

    In-flight statuses (``PENDING`` / ``CREATING`` / ``UPDATING`` /
    ``NOT_FOUND`` / unknown) keep polling and reset the consecutive-failure
    streak, so a single blip does not abort a healthy ramp-up. ``sleep`` /
    ``monotonic`` are injectable for tests.
    """
    import time

    _sleep = sleep or time.sleep
    _monotonic = monotonic or time.monotonic

    consecutive_failures = 0
    last_failure_detail = ""
    deadline = _monotonic() + timeout_sec

    def _record_failure(detail: str) -> Optional[dict]:
        nonlocal consecutive_failures, last_failure_detail
        consecutive_failures += 1
        last_failure_detail = detail
        if consecutive_failures >= max_consecutive_failures:
            return {
                "status": "error",
                "error": detail or "Online service status polling failed repeatedly",
            }
        return None

    while _monotonic() < deadline:
        _sleep(interval_sec)
        try:
            status = poll_status()
        except Exception as exc:  # noqa: BLE001 — poll is best-effort (see cap)
            log.debug("online-service status poll raised: %s", exc, exc_info=True)
            aborted = _record_failure(str(exc))
            if aborted is not None:
                return aborted
            continue

        current = status.get("status", "unknown")
        message = status.get("message", "")
        progress.update(current, message)

        if current == "RUNNING":
            return {"status": "RUNNING", "message": _SERVICE_WAIT_SUCCESS_MESSAGE}

        if current in _SERVICE_WAIT_FAILED_STATUSES:
            return {
                "status": current,
                "error": message or f"Online service reported {current}",
            }

        if current == "error":
            # Manager query-failure envelope (transient SQL / privileges): log
            # and count toward the cap rather than aborting on the first blip.
            detail = status.get("error", "") or message
            log.debug("online-service status poll returned error envelope: %s", detail)
            aborted = _record_failure(detail)
            if aborted is not None:
                return aborted
            continue

        # In-flight: a good poll clears the transient-failure streak.
        consecutive_failures = 0

    return {"status": "timeout", "error": f"Timed out after {int(timeout_sec)}s"}


def _print_target_header(result: dict) -> None:
    """Print the resolved manifest target + warehouse to stderr."""
    db = _term(result.get("target_database", ""))
    schema = _term(result.get("target_schema", ""))
    wh = _term(result.get("target_warehouse", ""))
    name = _term(result.get("target_name", ""))
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
    sys.stderr.write(f"\nDatabase: {_term(db)}  Schema: {_term(schema)}\n\n")
    sys.stderr.flush()


def _print_status_header(result: dict) -> None:
    """Print the ``Status: ...  Operations: ...`` header to stderr.

    Header on stderr, payload on stdout, so it never pollutes structured output.
    """
    status = result.get("status", "")
    if not status:
        return
    status = _term(status)
    ops = _actionable_ops(result.get("ops", []))
    total = len(ops)
    executed_value = result.get("executed")
    if executed_value is None:
        executed_value = sum(1 for o in ops if o.get("status") == "success")
    sys.stderr.write(
        f"Status: {status}  Operations: {total} (executed: {executed_value})\n"
    )
    sys.stderr.flush()


def _resolve_export_python(python_form: bool, yaml_form: bool) -> bool:
    """Resolve export form from ``--python`` / ``--yaml``; Python is default, both is an error."""
    if python_form and yaml_form:
        raise CliArgumentError("Pass only one of --python / --yaml.")
    return not yaml_form


# ---------------------------------------------------------------------------
# init — single bootstrap command (subsumes the deleted `export` command)
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@feature_store_preflight
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
    python_form: bool = typer.Option(
        False,
        "--python",
        help="Export deployed objects as .py files (Pydantic constructors).  This is the default form; pass --yaml to export YAML instead.",
    ),
    yaml_form: bool = typer.Option(
        False,
        "--yaml",
        help="Export deployed objects as YAML files instead of the default .py Pydantic constructors.",
    ),
    **options,
) -> CommandResult:
    """Bootstrap a feature-store project and pull deployed artifacts.

    Always runs in the current directory. Idempotent: re-running preserves
    the existing ``manifest.yml`` and refreshes the on-disk artifacts.
    Objects export as ``.py`` (Pydantic constructors) by default, or YAML
    with ``--yaml``. On a re-init, ``--database`` / ``--schema`` values that
    differ from the resolved manifest target are rejected (edit the manifest).
    """
    export_python = _resolve_export_python(python_form, yaml_form)
    db_override: Optional[str] = options.get("database")
    sch_override: Optional[str] = options.get("schema")
    del options
    result = FeatureManager().init(
        project_root=Path.cwd(),
        target_name=target,
        database=db_override,
        schema=sch_override,
        python=export_python,
    )
    _print_warnings(result)
    return _to_object(result)


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@feature_store_preflight
@_surface_init_required_as_cli_error
def sync(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    name: Optional[str] = typer.Option(
        None,
        "--name",
        help=(
            "Exact name of a single object to sync (case-insensitive). "
            "Matches across all object kinds (Entity, BatchSource, "
            "StreamingSource, BatchFeatureView, StreamingFeatureView, "
            "FeatureGroup).  Omit to sync all deployed objects."
        ),
        show_default=False,
    ),
    python_form: bool = typer.Option(
        False,
        "--python",
        help="Export deployed objects as .py files (Pydantic constructors).  This is the default form; pass --yaml to export YAML instead.",
    ),
    yaml_form: bool = typer.Option(
        False,
        "--yaml",
        help="Export deployed objects as YAML files instead of the default .py Pydantic constructors.",
    ),
    **options,
) -> CommandResult:
    """Pull deployed feature-store objects into the local sources tree.

    Requires an existing ``manifest.yml`` and an initialised feature store;
    unlike ``init`` it does not re-bootstrap the runtime. Use ``--name`` to
    sync a single object; ``--yaml`` emits YAML instead of ``.py``.
    """
    export_python = _resolve_export_python(python_form, yaml_form)
    del options
    result = FeatureManager().sync(
        from_dir=from_location,
        target_name=target,
        name_filter=name,
        python=export_python,
    )
    _print_warnings(result)
    return _to_object(result)


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@feature_store_preflight
@_surface_init_required_as_cli_error
def apply(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    destructive: bool = typer.Option(
        False,
        "--destructive",
        help="Allow operations that drop, recreate, or overwrite existing objects.",
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

    A pure plan-file consumer: auto-discovers the latest unapplied plan
    under ``<project_root>/out/plan/`` (or consumes ``--plan <path>``).
    Run ``snow feature plan`` first to produce a plan file.

    Templating overrides are a plan-time concern only: they are resolved
    and baked into the plan JSON, so ``apply`` does not accept them. Re-run
    ``snow feature plan`` with the new override, then ``apply``.
    """
    result = FeatureManager().apply(
        from_dir=from_location,
        target_name=target,
        plan_file=plan,
        destructive=destructive,
    )
    _print_target_header(result)
    _print_status_header(result)

    # Terminal failure (target_mismatch / partial_failure / refused /
    # validation_failed): keep the human ops table + diagnostics in TABLE
    # mode, keep the full envelope on structured stdout, and exit non-zero.
    if _is_terminal_failure(result):
        if not _is_structured_output():
            if result.get("errors"):
                _print_diagnostics(result)
            _emit(_ops_result(result))
        _fail_terminal(result)

    # Success / idle (applied / no_plan): in structured mode with no
    # actionable ops, return the full envelope so the status + any errors
    # reach the caller instead of collapsing to ``{"message": "Operations: 0"}``.
    if _is_structured_output() and not _actionable_ops(result.get("ops", [])):
        return _to_object(result)
    if result.get("errors") and not _is_structured_output():
        _print_diagnostics(result)
    return _ops_result(result)


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@feature_store_preflight
@_surface_init_required_as_cli_error
def plan(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    variables: Optional[List[str]] = variables_option,
    out: Optional[str] = typer.Option(
        None,
        "--out",
        help="Path to write the plan JSON file. Defaults to "
        "`<project_root>/out/plan/feature_plan_<timestamp>.json`.",
        show_default=False,
    ),
    no_delete: bool = typer.Option(
        True,
        "--no-delete/--delete",
        help="Deletion detection is OFF by default: objects present in the "
        "target but absent from the manifest are left untouched. Pass "
        "``--delete`` to enable full-sync deletion (emit DROP ops for orphans).",
    ),
    **options,
) -> CommandResult:
    """Show what would change if the project were applied (read-only).

    Also writes the plan as JSON under ``<project_root>/out/plan/`` so it
    can be applied later with ``snow feature apply``.
    """
    manager = FeatureManager()
    # One generate: ``plan`` returns the UI envelope AND the generated Plan so
    # the ops table shown and the JSON written to ``out/plan/`` come from the
    # same fetch (no double ``DESCRIBE``; no TOCTOU gap between display and the
    # file ``apply`` will execute).
    result, plan_obj = manager.plan(
        from_dir=from_location,
        target_name=target,
        variables=variables,
        destructive=False,
        no_delete=no_delete,
    )
    _print_target_header(result)
    if _is_terminal_failure(result):
        _print_status_header(result)
        # Terminal validation failure: envelope on structured stdout, readable
        # diagnostics + compact envelope in TABLE mode, non-zero exit, and no
        # plan file written.
        if not _is_structured_output():
            _print_diagnostics(result)
            _emit(_to_object(_compact_failure_envelope(result)))
        _fail_terminal(result)

    # Serialize the very plan the operator just saw — no regenerate, no refetch.
    plan_path = manager.write_plan_object(
        plan_obj,
        from_dir=from_location,
        target_name=target,
        out_path=out,
    )
    result["plan_file"] = plan_path
    _print_status_header(result)
    # A ready plan with no actionable ops still returns the full envelope in
    # structured mode (status + plan_file) rather than ``Operations: 0``.
    if _is_structured_output() and not _actionable_ops(result.get("ops", [])):
        return _to_object(result)
    return _ops_result(result)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@app.command(name="list", requires_connection=True)
@feature_store_preflight
@_surface_init_required_as_cli_error
def list_cmd(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
) -> CommandResult:
    """List deployed feature-store objects from Snowflake."""
    result = FeatureManager().list_specs(from_dir=from_location, target_name=target)
    specs = result.get("specs", [])
    if isinstance(specs, list) and specs and isinstance(specs[0], dict):
        if not _is_structured_output():
            _print_listing_scope_header(specs)
        return _to_collection(specs)
    # A backend SQL error (``status='error'``) is a terminal failure: keep the
    # envelope on structured stdout and exit non-zero. An empty-but-valid store
    # (no ``status``) still returns the object with exit 0.
    if _is_terminal_failure(result):
        _fail_terminal(result)
    return _to_object(result)


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@feature_store_preflight
@_surface_init_required_as_cli_error
def describe(
    name: str = typer.Argument(
        ...,
        help="Feature store object (e.g. 'user_event_features'). "
        "Also accepts the full OFT name (NAME$VERSION$ONLINE).",
        show_default=False,
    ),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    version: Optional[str] = typer.Option(
        None,
        "--version",
        help="Feature store object version (e.g. 'V1'). Use to disambiguate "
        "objects that share a base name but differ only by version. "
        "Required when multiple versions of the named object are deployed.",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """Describe a single feature-store object."""
    result = FeatureManager().describe(
        from_dir=from_location, target_name=target, name=name, version=version
    )

    # ``_display`` banner is TABLE-only; strip it so it never leaks into
    # structured stdout.
    _write_display(result.pop("_display", None))

    # A not-found / unresolved describe (``status='error'``) is a terminal
    # failure: keep the ``status`` / ``error`` envelope on structured stdout
    # and exit non-zero.
    if _is_terminal_failure(result):
        _fail_terminal(result)

    if _is_structured_output():
        return _to_object(result)

    # TABLE mode: the stderr banner is the canonical surface for a
    # successful describe, so emit nothing on stdout; error envelopes (no
    # rows) still render as a key/value object so the failure is visible.
    result.pop("examples", None)
    rows = result.get("rows", [])
    if isinstance(rows, list) and rows:
        return EmptyResult()
    return _to_object(result)


# ---------------------------------------------------------------------------
# online-service
# ---------------------------------------------------------------------------

_DROP_CONFIRM_COMMAND = "DROP"
_DROP_CANCEL_COMMAND = "CANCEL"


def _confirm_drop(location: str) -> None:
    """Require a typed ``drop <DATABASE.SCHEMA>`` confirmation (DCM purge parity).

    ``online-service drop`` tears down every Online Feature Table and the
    online-service runtime in ``location`` with no undo, so — like
    ``snow dcm purge`` — the
    interactive default demands the operator retype the target. ``cancel``
    (or an empty prompt loop) aborts; a matching ``drop <location>`` returns.
    """
    cli_console.warning(
        f"\u26a0\ufe0f  DANGER: This drops ALL Online Feature Tables and the "
        f"online-service runtime in {location}  \u26a0\ufe0f"
    )
    while True:
        user_input = typer.prompt(
            f"Type 'drop {location}' to confirm or 'cancel' to abort",
            show_default=False,
        )
        parts = user_input.strip().split(maxsplit=1)
        if not parts:
            continue

        command = parts[0].upper()

        if command == _DROP_CANCEL_COMMAND:
            raise typer.Abort()

        if command == _DROP_CONFIRM_COMMAND and len(parts) == 2:
            if parts[1].upper() == location.upper():
                return
            cli_console.message(
                f"  Location mismatch. Expected: {location}, provided: {parts[1]}"
            )


# ``online-service`` is its own entity with a lifecycle (status / create /
# drop), so it is a nested sub-group rather than one command configured by
# flags: the operations are alternate verbs, not configuration of a single
# command (see ``docs/contributing/adding-commands.md``). ``preview=True``
# matches the parent factory; it is not inherited through ``add_typer``.
online_service_app = SnowTyperFactory(
    name="online-service",
    help="Manages the feature store online service.",
    preview=True,
)
app.add_typer(online_service_app)


@online_service_app.command(name="status", requires_connection=True)
@feature_store_preflight
def online_service_status(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
) -> CommandResult:
    """Show the feature store online service runtime status.

    Status is compact by default; pass ``-v`` / ``--verbose`` for the full
    layout. The location comes from the manifest target, which must be
    reachable; an explicit ``--target`` against a manifest-less directory is
    always a hard error.
    """
    result = FeatureManager().get_status(from_dir=from_location, target_name=target)
    # An unreachable manifest or a failed status query is a terminal
    # failure, not a status to render: an ``error`` envelope carries no
    # ``_display``, so there is nothing human to show either. Emit the
    # full envelope and exit non-zero, per the terminal-failure contract.
    if _is_terminal_failure(result):
        _fail_terminal(result)
    # ``get_status`` builds the rich TABLE banner on ``_display``
    # (adapter parity with ``describe``): the library formatter is
    # invoked from the manager, so the command only routes the banner
    # and never dereferences ``decl_api`` for formatting. Pop the
    # display-only keys so they never leak into structured stdout
    # (older manager builds also set ``_user`` / ``_database`` /
    # ``_schema``).
    display = result.pop("_display", None)
    result.pop("_user", None)
    result.pop("_database", None)
    result.pop("_schema", None)
    if _is_structured_output():
        return _to_object(result)
    # TABLE: the stderr banner is the canonical surface, so emit
    # nothing on stdout.
    _write_display(display)
    return EmptyResult()


@online_service_app.command(name="create", requires_connection=True)
@feature_store_preflight
def online_service_create(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
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
    """Create and initialize the feature store online service.

    Idempotent: a no-op when the service is already RUNNING. This is the only
    online-service command that falls back to the connection's database /
    schema when no ``manifest.yml`` is reachable, so a runtime can be stood up
    before a project exists; an explicit ``--target`` against a manifest-less
    directory is always a hard error.
    """
    mgr = FeatureManager()
    pre_status = mgr.get_status(
        from_dir=from_location,
        target_name=target,
        allow_connection_fallback=True,
    )
    if pre_status.get("status") == "RUNNING":
        return _to_object(
            {
                "status": "RUNNING",
                "message": "Service already initialized",
            }
        )

    # Progress is human-facing only (stderr, suppressed for structured
    # / ``--silent`` output); the bar clears itself on context exit.
    with _service_wait_progress("CREATING", "Sending create request...") as progress:
        result = mgr.initialize_service(
            from_dir=from_location,
            target_name=target,
            producer_role=producer_role,
            consumer_role=consumer_role,
        )

        # A failed CREATE request (bad role, missing privileges, warehouse
        # unavailable) is terminal: emit the envelope and exit non-zero so
        # ``create`` followed by ``apply`` cannot proceed against a runtime
        # that was never created.
        if _is_terminal_failure(result):
            _fail_terminal(result)

        if result.get("status") == "RUNNING":
            return _to_object(result)

        progress.update("CREATING", "Waiting for service to start...")

        def _poll_status() -> dict:
            return mgr.get_status(
                from_dir=from_location,
                target_name=target,
                allow_connection_fallback=True,
            )

        wait_result = _wait_online_service_running(_poll_status, progress)

    # ``get_status`` now stamps a display-only ``_display`` banner; strip it
    # so it never rides into structured stdout (success or ``_fail_terminal``).
    wait_result.pop("_display", None)

    # Success is exit-0; every other terminal outcome (FAILED / SUSPENDED /
    # ERROR / repeated poll failure / timeout) emits the full envelope and
    # exits non-zero, per the terminal-failure contract in DESIGN.md.
    if wait_result.get("status") == "RUNNING":
        return _to_object(wait_result)
    _fail_terminal(wait_result)


@online_service_app.command(name="drop", requires_connection=True)
@feature_store_preflight
def online_service_drop(
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """Destroy the online service and all Online Feature Tables.

    Destructive: requires a reachable manifest (no connection fallback) and,
    unless ``--force`` is passed, a typed confirmation of the resolved
    ``DATABASE.SCHEMA``. An explicit ``--target`` against a manifest-less
    directory is always a hard error.
    """
    # Destructive teardown gate (DCM ``purge`` parity): refuse a
    # non-interactive drop without ``--force``, and otherwise require
    # a typed confirmation of the resolved ``DATABASE.SCHEMA``. ``--force``
    # skips the prompt for scripts / CI.
    mgr = FeatureManager()
    if not force and not interactive:
        raise CliError(
            "Cannot drop the online service non-interactively without --force."
        )
    if not force:
        database, schema, _ = mgr._resolve_service_target(  # noqa: SLF001
            from_location, target
        )
        _confirm_drop(f"{database}.{schema}")

    result = mgr.destroy_service(from_dir=from_location, target_name=target)
    # A failed teardown (``partial_failure`` / ``failed``) emits the full
    # envelope and exits non-zero, per the terminal-failure contract;
    # ``destroyed`` falls through to the exit-0 object below.
    if _is_terminal_failure(result):
        _fail_terminal(result)
    return _to_object(result)


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@feature_store_preflight
@_surface_init_required_as_cli_error
def ingest(
    source_name: str = typer.Argument(
        ...,
        help="Name of the streaming source to ingest records into.",
        show_default=False,
    ),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
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
            content = SecurePath(data).read_text(
                file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB
            )
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
        raise CliError(_term(str(exc)))
    return _to_object(result)


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


@app.command(requires_connection=True)
@feature_store_preflight
@_surface_init_required_as_cli_error
def query(
    feature_view_name: str = typer.Argument(
        ...,
        help="Name of the feature view to query.",
        show_default=False,
    ),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    version: str = typer.Option(
        ...,
        "--version",
        help=(
            "Feature view version (e.g. 'V1').  Required because "
            "the online lookup is keyed on (name, version) — "
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
            from_dir=from_location,
            target_name=target,
            feature_view_name=feature_view_name,
            version=version,
            keys=parsed_keys,
        )
    except RuntimeError as exc:
        raise CliError(_term(str(exc)))
    return _to_object(result)
