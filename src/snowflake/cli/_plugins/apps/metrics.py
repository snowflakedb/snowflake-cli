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
"""Query-ID telemetry for the Snowflake App Runtime (``snowflake-app``) flow.

The shared metrics layer (:class:`snowflake.cli.api.metrics.CLIMetricsSpan`)
emits a fixed set of keys per span and has no API for attaching custom
metadata. We want every SQL statement issued by ``snow app`` commands to be
correlatable to the span it ran under via its Snowflake query ID (``sfqid``),
*without* changing the shared metrics schema for the rest of the CLI.

This module keeps that change scoped entirely to the apps plugin:

* :func:`record_query_id` stores a query ID on the *currently active* span as
  a private instance attribute, so it travels with the span object the rest
  of the CLI already tracks.
* :func:`install_query_id_telemetry` wraps ``CLIMetricsSpan.to_dict`` once so
  that the collected IDs are surfaced under a ``query_ids`` key in the
  telemetry payload — but only for spans that actually recorded one. Spans
  that never opt in keep their existing schema byte-for-byte, so no other
  plugin's telemetry is affected.

The wrapping is installed automatically on import of this module (which the
apps manager/commands always import), so callers only need to invoke
:func:`record_query_id`.
"""

from __future__ import annotations

import functools
import logging
from typing import Any, List, Optional

from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.metrics import CLIMetricsSpan

log = logging.getLogger(__name__)

# Key under which query IDs are emitted in the span's telemetry dict.
QUERY_IDS_KEY = "query_ids"
# Private attribute used to stash collected IDs on a span instance. Prefixed
# to avoid colliding with any future field added to the shared dataclass.
_QUERY_IDS_ATTR = "_app_query_ids"
# Sentinel so :func:`install_query_id_telemetry` is idempotent even if this
# module is re-imported / re-executed (e.g. across test runs).
_WRAPPED_FLAG = "_app_query_id_wrapped"


def record_query_id(query_id: Optional[str]) -> None:
    """Attach a Snowflake query ID (``sfqid``) to the active metrics span.

    Falsy values are ignored so callers can pass ``cursor.sfqid`` (or an
    error's ``sfqid``) unconditionally. When no span is currently open the
    call is a no-op — SQL run outside of a ``metrics.span(...)`` block simply
    isn't correlated to anything.

    Telemetry must never break command execution, so any unexpected failure
    here is swallowed and logged at debug level.
    """
    if not query_id:
        return
    try:
        span = get_cli_context().metrics.current_span
        if span is None:
            return
        ids: Optional[List[str]] = getattr(span, _QUERY_IDS_ATTR, None)
        if ids is None:
            ids = []
            setattr(span, _QUERY_IDS_ATTR, ids)
        ids.append(query_id)
    except Exception:  # pragma: no cover - defensive; telemetry is best-effort
        log.debug("Failed to record query ID for telemetry", exc_info=True)


def record_query_id_from_cursor(cursor: Any) -> None:
    """Record ``cursor.sfqid`` (if any) onto the active span."""
    record_query_id(getattr(cursor, "sfqid", None))


def get_recorded_query_ids(span: CLIMetricsSpan) -> List[str]:
    """Return the query IDs recorded on ``span`` (empty list if none).

    Exposed primarily for tests and introspection.
    """
    return list(getattr(span, _QUERY_IDS_ATTR, []) or [])


def install_query_id_telemetry() -> None:
    """Augment ``CLIMetricsSpan.to_dict`` to emit recorded query IDs.

    Idempotent: re-invocation (or re-import of this module) is a no-op. The
    wrapper only adds the ``query_ids`` key when the span recorded at least
    one ID, leaving the payload identical for every span that didn't opt in.
    """
    original_to_dict = CLIMetricsSpan.to_dict
    if getattr(original_to_dict, _WRAPPED_FLAG, False):
        return

    @functools.wraps(original_to_dict)
    def to_dict_with_query_ids(self: CLIMetricsSpan) -> dict:
        data = original_to_dict(self)
        ids = getattr(self, _QUERY_IDS_ATTR, None)
        if ids:
            data[QUERY_IDS_KEY] = list(ids)
        return data

    setattr(to_dict_with_query_ids, _WRAPPED_FLAG, True)
    CLIMetricsSpan.to_dict = to_dict_with_query_ids  # type: ignore[method-assign]


# Install on import so any code path that touches the apps plugin gets the
# augmented serialization without an explicit setup step.
install_query_id_telemetry()
