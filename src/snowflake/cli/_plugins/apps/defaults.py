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

"""AppDefaults NamedTuple and resolution logic for Snowflake Apps.

Centralises the four-tier resolution order used by both ``setup`` and
``deploy`` commands:

    user input > account parameter > default > current session

Each tier is represented as an ``AppDefaults`` instance with a *source*
label.  ``resolve_defaults()`` merges them in order and returns both
the winning values and a human-readable provenance summary.
"""

from __future__ import annotations

from typing import Dict, List, NamedTuple, Optional, Tuple

# ── Source provenance labels ──────────────────────────────────────────
SOURCE_USER_INPUT = "user input"
SOURCE_ACCOUNT_PARAM = "account parameter"
SOURCE_DEFAULT = "default"
SOURCE_CURRENT_SESSION = "current session"
SOURCE_MISSING = "missing"

# Fixed precedence order (highest → lowest).
RESOLUTION_ORDER: List[str] = [
    SOURCE_USER_INPUT,
    SOURCE_ACCOUNT_PARAM,
    SOURCE_DEFAULT,
    SOURCE_CURRENT_SESSION,
]

# Value field names on AppDefaults (everything except ``source``).
_VALUE_FIELDS: Tuple[str, ...] = (
    "warehouse",
    "build_compute_pool",
    "service_compute_pool",
    "build_eai",
    "database",
    "schema",
)


class AppDefaults(NamedTuple):
    """Immutable bag of resolved (or partially-resolved) Snowflake App defaults.

    Each instance carries a ``source`` label describing where the values
    came from (e.g. ``SOURCE_ACCOUNT_PARAM``).  Fields that are not
    provided by this source are ``None``.
    """

    source: str = SOURCE_MISSING
    warehouse: Optional[str] = None
    build_compute_pool: Optional[str] = None
    service_compute_pool: Optional[str] = None
    build_eai: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None

    def _clean(self) -> "AppDefaults":
        """Return a copy with empty strings converted to None."""
        cleaned = {f: (getattr(self, f) or None) for f in _VALUE_FIELDS}
        return self._replace(**cleaned)

    def to_dict(self) -> Dict[str, str]:
        """Return non-None value fields as a dict (excludes ``source``)."""
        return {
            f: getattr(self, f) for f in _VALUE_FIELDS if getattr(self, f) is not None
        }

    def has_values(self) -> bool:
        """Return ``True`` if at least one value field is non-None."""
        return bool(self.to_dict())

    def summary(self) -> str:
        """One-line summary: ``source: field=value, ...`` (non-None only)."""
        parts = [f"{f}={v}" for f, v in self.to_dict().items() if v is not None]
        return (
            f"{self.source}: {', '.join(parts)}" if parts else f"{self.source}: (empty)"
        )


def resolve_defaults(
    defaults: List[AppDefaults],
) -> Tuple[AppDefaults, str]:
    """Merge multiple ``AppDefaults`` using :data:`RESOLUTION_ORDER`.

    Parameters
    ----------
    defaults:
        One ``AppDefaults`` per source tier that is available.  Their
        ``source`` labels must appear in ``RESOLUTION_ORDER``; unknown
        sources are silently skipped.

    Returns
    -------
    (resolved, provenance_summary)
        *resolved* is an ``AppDefaults`` with ``source="resolved"`` whose
        fields are the first non-``None`` value from the highest-priority
        source.  *provenance_summary* is a multi-line string showing which
        source won for each field.
    """
    by_source: Dict[str, AppDefaults] = {d.source: d._clean() for d in defaults}

    resolved_vals: Dict[str, Optional[str]] = {}
    provenance: Dict[str, str] = {}

    for field in _VALUE_FIELDS:
        for source_label in RESOLUTION_ORDER:
            tier = by_source.get(source_label)
            if tier is not None:
                val = getattr(tier, field, None)
                if val is not None:
                    resolved_vals[field] = val
                    provenance[field] = source_label
                    break
        else:
            resolved_vals[field] = None
            provenance[field] = SOURCE_MISSING

    summary_lines = [f"  {k}: {v}  ({provenance[k]})" for k, v in resolved_vals.items()]
    provenance_summary = "\n".join(summary_lines)

    resolved = AppDefaults(source="resolved", **resolved_vals)
    return resolved, provenance_summary
