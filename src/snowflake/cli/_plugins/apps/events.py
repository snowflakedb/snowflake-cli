# Copyright (c) 2026 Snowflake Inc.
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

"""Parsing and presentation helpers for ``snow app events``.

The Snowflake App Runtime flow of ``snow app events`` reads observability
telemetry (logs, metrics, and lifecycle events) for an application service out
of its event table via the ``SYSTEM$GET_APPLICATION_SERVICE_EVENT_TABLE_DATA``
system function. That function returns a VARCHAR holding a JSON array of
positional tuples whose layout depends on the requested ``event_type``.

This module keeps the pure logic — client-side time-window resolution and
decoding the positional tuples into named, human-readable records — separate
from the command wiring so it can be unit-tested without a Snowflake
connection.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List, Optional, Tuple

from snowflake.cli.api.exceptions import CliError

log = logging.getLogger(__name__)

# Name of the system function that exposes event-table telemetry for an
# application service. Accepts an app FQN, an event type, and an optional
# ``[start_time, end_time]`` window of ``TIMESTAMP`` literals.
EVENT_TABLE_FUNCTION = "SYSTEM$GET_APPLICATION_SERVICE_EVENT_TABLE_DATA"

# Default look-back applied when the user requests a historical stream without
# giving an explicit ``--since``. See :func:`resolve_time_window`.
DEFAULT_LOOKBACK = timedelta(hours=1)

# ``TIMESTAMP`` literal format passed to the system function for its window
# bounds. The function interprets a *naive* literal in the session timezone, so
# we emit an explicit UTC (``...Z``) ISO-8601 literal — the client resolves all
# windows in UTC, keeping results correct regardless of the session timezone.
_TIMESTAMP_LITERAL_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Only the workload container carries the resource/network telemetry we surface.
_RUNNER_CONTAINER = "runner"

# Positions within a METRIC tuple: [ts, name, value, unit, instance, container, ...].
_METRIC_TS = 0
_METRIC_NAME = 1
_METRIC_VALUE = 2
_METRIC_UNIT = 3
_METRIC_CONTAINER = 5

# Positions within a LOG tuple: [ts, instance, container, body, attributes].
_LOG_TS = 0
_LOG_CONTAINER = 2
_LOG_BODY = 3

# Positions within an EVENT (lifecycle) tuple:
# [ts, severity, name, body_json, instance, container, scope_json, ...].
# ``body_json`` is a JSON object carrying ``message`` and ``status``.
_EVENT_TS = 0
_EVENT_SEVERITY = 1
_EVENT_NAME = 2
_EVENT_BODY = 3
_EVENT_CONTAINER = 5

_BINARY_UNITS = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")


class EventStream(str, Enum):
    """The ``--type`` value chosen by the user (CLI-facing name)."""

    LOG = "log"
    METRIC = "metric"
    # ``lifecycle`` maps to the function's ``EVENT`` type; renamed to avoid the
    # "events within app events" confusion.
    LIFECYCLE = "lifecycle"

    @property
    def event_table_type(self) -> str:
        """The ``event_type`` argument passed to the system function."""
        return "EVENT" if self is EventStream.LIFECYCLE else self.name


class MetricCategory(str, Enum):
    """The ``--metric`` subset applied to the METRIC stream."""

    CPU = "cpu"
    MEMORY = "memory"
    NETWORK = "network"

    @property
    def name_prefix(self) -> str:
        """The metric-name prefix used to filter this category.

        CPU and memory telemetry is emitted under ``container.<category>.``;
        network telemetry is emitted under ``network.`` (e.g.
        ``network.ingress.cps``).
        """
        if self is MetricCategory.NETWORK:
            return "network."
        return f"container.{self.value}."


def parse_event_stream(value: Optional[str]) -> EventStream:
    """Resolve a user-supplied ``--type`` string to an :class:`EventStream`."""
    if not value:
        return EventStream.LOG
    try:
        return EventStream(value.lower())
    except ValueError:
        valid = ", ".join(s.value for s in EventStream)
        raise CliError(
            f"Invalid --type '{value}'. Valid values for Snowflake App Runtime "
            f"projects are: {valid}."
        )


def parse_metric_category(value: Optional[str]) -> Optional[MetricCategory]:
    """Resolve a user-supplied ``--metric`` string to a :class:`MetricCategory`."""
    if not value:
        return None
    try:
        return MetricCategory(value.lower())
    except ValueError:
        valid = ", ".join(c.value for c in MetricCategory)
        raise CliError(f"Invalid --metric '{value}'. Valid values are: {valid}.")


# ── Time-window resolution ────────────────────────────────────────────

_RELATIVE_RE = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$", re.IGNORECASE)

_RELATIVE_UNIT_SECONDS = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
}

# Absolute timestamp formats accepted for ``--since`` / ``--until``.
_ABSOLUTE_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
)


def _parse_bound(value: str, now: datetime) -> datetime:
    """Parse a single ``--since`` / ``--until`` bound.

    Accepts relative shorthand (``30m``, ``6h``, ``2d``, ``1w``) interpreted as
    "that much time before *now*", or an absolute timestamp in one of the
    supported formats (interpreted as UTC).
    """
    relative = _RELATIVE_RE.match(value)
    if relative:
        amount = int(relative.group(1))
        unit = relative.group(2).lower()
        return now - timedelta(seconds=amount * _RELATIVE_UNIT_SECONDS[unit])

    for fmt in _ABSOLUTE_FORMATS:
        try:
            parsed = datetime.strptime(value.strip(), fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    raise CliError(
        f"Could not parse time value '{value}'. Use relative shorthand "
        "(e.g. 30m, 6h, 2d) or an absolute timestamp (e.g. "
        "'2026-07-16 18:00:00')."
    )


def resolve_time_window(
    since: Optional[str],
    until: Optional[str],
    *,
    default_lookback: timedelta = DEFAULT_LOOKBACK,
    now: Optional[datetime] = None,
) -> Tuple[str, str]:
    """Resolve ``--since`` / ``--until`` to the literal window the function needs.

    The bounds default so that a partial window is still usable:

    * neither bound → ``[now - default_lookback, now]``
    * ``--since`` only → ``[since, now]``
    * ``--until`` only → ``[until - default_lookback, until]``
    * both → ``[since, until]``

    Returns a ``(start_time, end_time)`` pair of ``TIMESTAMP`` literal strings.
    """
    now = now or datetime.now(timezone.utc)
    end_dt = _parse_bound(until, now) if until else now
    start_dt = _parse_bound(since, now) if since else end_dt - default_lookback

    if start_dt > end_dt:
        raise CliError(
            "The start of the requested window is after its end. Check "
            "--since and --until."
        )

    return (
        start_dt.strftime(_TIMESTAMP_LITERAL_FORMAT),
        end_dt.strftime(_TIMESTAMP_LITERAL_FORMAT),
    )


# ── Raw response decoding ─────────────────────────────────────────────


def _load_rows(raw: str) -> List[list]:
    """Decode the VARCHAR payload into a list of positional tuples.

    The system function returns an empty/null payload when there is no data in
    the window; treat those as an empty result rather than an error.
    """
    if not raw:
        return []
    try:
        rows = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        log.debug("Could not decode event-table payload as JSON", exc_info=True)
        return []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, list)]


def _epoch_to_iso(value: str) -> str:
    """Convert an epoch-seconds string (with optional ns decimals) to ISO UTC."""
    try:
        dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return str(value)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def format_bytes(num_bytes: float) -> str:
    """Render a byte count as a binary-prefixed, human-readable string."""
    value = float(num_bytes)
    for unit in _BINARY_UNITS:
        if abs(value) < 1024.0 or unit == _BINARY_UNITS[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} {_BINARY_UNITS[-1]}"


def format_cores(num_cores: float) -> str:
    """Render a CPU-core count as a human-readable string."""
    return f"{float(num_cores):g} cores"


def _metric_value_fields(unit: str, raw_value: float, raw: bool) -> dict:
    """Build the value fields for a metric record.

    Each record carries a human-readable ``value`` plus the raw numeric value
    under a unit-appropriate key (``bytes`` / ``cores``) so the output is
    readable yet still script-friendly. With ``raw=True`` the ``value`` field
    reports the raw number instead of the converted string.
    """
    unit = (unit or "").lower()
    if unit == "byte":
        raw_num: float = int(raw_value)
        human = format_bytes(raw_num)
        raw_key = "bytes"
    elif unit == "cpu":
        raw_num = raw_value
        human = format_cores(raw_num)
        raw_key = "cores"
    else:
        raw_num = raw_value
        human = str(raw_value)
        raw_key = "raw"
    return {"value": raw_num if raw else human, raw_key: raw_num}


def parse_metric_records(
    raw: str,
    *,
    category: Optional[MetricCategory] = None,
    raw_values: bool = False,
) -> List[dict]:
    """Decode a METRIC payload into named records.

    Optionally filters to a single ``--metric`` *category* (cpu / memory /
    network). Records are returned latest-first.
    """
    records: List[dict] = []
    for row in _load_rows(raw):
        if len(row) <= _METRIC_CONTAINER:
            continue
        name = row[_METRIC_NAME]
        if category is not None and not str(name).startswith(category.name_prefix):
            continue
        try:
            numeric = float(row[_METRIC_VALUE])
        except (TypeError, ValueError):
            continue
        record = {
            "time": _epoch_to_iso(row[_METRIC_TS]),
            "metric": name,
        }
        record.update(_metric_value_fields(row[_METRIC_UNIT], numeric, raw_values))
        record["container"] = row[_METRIC_CONTAINER]
        records.append(record)

    records.sort(key=lambda r: r["time"], reverse=True)
    return records


def _event_body_fields(body) -> dict:
    """Extract ``status`` / ``message`` from a lifecycle tuple's body field.

    The body position holds a JSON object like
    ``{"message": "Service is ready", "status": "RUNNING"}``. Fall back to the
    raw string if it is not decodable JSON.
    """
    if isinstance(body, str):
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            return {"status": "", "message": body}
    elif isinstance(body, dict):
        parsed = body
    else:
        return {"status": "", "message": ""}
    return {
        "status": str(parsed.get("status", "")),
        "message": str(parsed.get("message", "")),
    }


def parse_lifecycle_records(raw: str) -> List[dict]:
    """Decode an EVENT (lifecycle) payload into named records, latest-first.

    Each tuple describes a service / container status change. The record
    surfaces the event name (e.g. ``SERVICE.STATUS_CHANGE``), the new status,
    the human message, severity, and container (when the change is
    container-scoped).
    """
    records: List[dict] = []
    for row in _load_rows(raw):
        if len(row) <= _EVENT_BODY:
            continue
        record = {
            "time": _epoch_to_iso(row[_EVENT_TS]),
            "event": row[_EVENT_NAME],
        }
        record.update(_event_body_fields(row[_EVENT_BODY]))
        record["severity"] = row[_EVENT_SEVERITY]
        container = row[_EVENT_CONTAINER] if len(row) > _EVENT_CONTAINER else None
        record["container"] = container or ""
        records.append(record)
    records.sort(key=lambda r: r["time"], reverse=True)
    return records


def format_log_lines(raw: str) -> str:
    """Decode a LOG payload from the event table into printable text.

    Each tuple is ``[ts, instance, container, body, attributes]``; render one
    ``<iso-timestamp> <body>`` line per tuple, oldest first so the output reads
    like a normal log tail.
    """
    lines: List[str] = []
    for row in _load_rows(raw):
        if len(row) <= _LOG_BODY:
            continue
        timestamp = _epoch_to_iso(row[_LOG_TS])
        body = row[_LOG_BODY]
        lines.append(f"{timestamp} {body}".rstrip())
    return "\n".join(lines)
