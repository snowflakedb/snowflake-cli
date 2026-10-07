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

from enum import Enum
from logging import getLogger
from typing import Any

log = getLogger(__name__)

ALLOWED_PAYLOAD_KEYS = frozenset(
    {"event", "slot", "kind", "cache", "outcome", "latency_ms", "error_class"}
)

_EVENTS = frozenset({"requested", "accepted", "lookup"})
_SLOTS = frozenset({"keyword", "object", "column", "type", "none"})
_KINDS = frozenset(
    {
        "database",
        "schema",
        "table",
        "view",
        "column",
        "keyword",
        "function",
        "type",
        "none",
    }
)
_CACHES = frozenset({"hit", "miss", "negative", "none"})
_OUTCOMES = frozenset({"shown", "empty", "timeout", "error", "disabled"})
_ERROR_CLASSES = frozenset({"privilege", "network", "timeout", "internal"})


def _value(raw: Any) -> str:
    if isinstance(raw, Enum):
        return str(raw.value)
    return str(raw)


def build_completion_payload(
    *,
    event: str,
    slot: str,
    kind: str,
    cache: str,
    outcome: str,
    latency_ms: int | None = None,
    error_class: str | None = None,
) -> dict[str, str | int]:
    """Build a completion telemetry payload. Enums and numbers only."""
    payload: dict[str, str | int] = {
        "event": _require(_value(event), _EVENTS, "event"),
        "slot": _require(_value(slot), _SLOTS, "slot"),
        "kind": _require(_value(kind), _KINDS, "kind"),
        "cache": _require(_value(cache), _CACHES, "cache"),
        "outcome": _require(_value(outcome), _OUTCOMES, "outcome"),
    }
    if latency_ms is not None and payload["event"] == "lookup":
        payload["latency_ms"] = int(latency_ms)
    if error_class is not None:
        payload["error_class"] = _require(
            _value(error_class), _ERROR_CLASSES, "error_class"
        )
    return payload


def emit_completion(**kwargs) -> dict[str, str | int] | None:
    """Send a completion event. A bad allowlist value is dropped, not raised."""
    try:
        payload = build_completion_payload(**kwargs)
    except ValueError:
        log.debug("completion telemetry dropped", exc_info=True)
        return None
    _send(payload)
    return payload


def on_completion_accepted(*_args, **kwargs) -> None:
    """PTK accept hook. Enums only — ignore suggestion text and SQL."""
    emit_completion(
        event="accepted",
        slot=kwargs.get("slot", "none"),
        kind=kwargs.get("kind", "none"),
        cache="none",
        outcome=kwargs.get("outcome", "shown"),
    )


def _require(value: str, allowed: frozenset[str], field: str) -> str:
    if value not in allowed:
        raise ValueError(f"invalid completion telemetry {field}")
    return value


def _send(payload: dict[str, str | int]) -> None:
    """Hand the payload to the public REPL completion telemetry helper."""
    try:
        from snowflake.cli._app.telemetry import log_repl_completion

        log_repl_completion(payload)
    except Exception:
        log.debug("completion telemetry send failed", exc_info=True)


def drain() -> None:
    """Drain queued completion events on the UI thread, if a connection is open."""
    try:
        from snowflake.cli._app.telemetry import drain_repl_completions

        drain_repl_completions()
    except Exception:
        log.debug("completion telemetry drain failed", exc_info=True)
