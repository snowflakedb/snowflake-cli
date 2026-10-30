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

"""Client-side rollout policy from a signed snowflake-managed manifest.

Jenkins stamps ``policy`` and ``released_at``. ``staged`` ramps linearly
0→1 over ``STAGED_RAMP_HOURS`` from that timestamp; Releng does not bump
a fraction. Auto-upgrade includes a machine when its stable
``[0, 1)`` score is below that elapsed share. Manual ``snow upgrade``
ignores this. Missing or unknown policy is hold.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Protocol

log = logging.getLogger(__name__)

STAGED_RAMP_HOURS = 96
_STAGED_RAMP_SECONDS = STAGED_RAMP_HOURS * 3600


class RolloutPolicy(str, Enum):
    STAGED = "staged"
    IMMEDIATE = "immediate"
    HOLD = "hold"


@dataclass(frozen=True)
class Rollout:
    """Auto-upgrade policy for one published version.

    Membership is computed at check time from ``released_at`` and wall
    clock, not from a Releng-published fraction or percent buckets.
    """

    policy: RolloutPolicy
    released_at: Optional[datetime] = None


HOLD = Rollout(policy=RolloutPolicy.HOLD)
IMMEDIATE = Rollout(policy=RolloutPolicy.IMMEDIATE)


@dataclass(frozen=True)
class PublishedRelease:
    """Pointer version plus rollout from that version's signed manifest."""

    version: str
    rollout: Rollout


class ReleaseSource(Protocol):
    """Optional extra on ``VersionSource``. ``HttpRepo`` implements this."""

    def latest_release(self) -> PublishedRelease:
        ...


def parse_rollout(manifest_dict: object) -> Rollout:
    """Read ``rollout`` from a verified manifest. Never raises; garbage is hold."""
    try:
        return _parse_rollout(manifest_dict)
    except Exception:
        log.debug("Treating unreadable rollout as hold", exc_info=True)
        return HOLD


def in_slice(score: float, rollout: Rollout, now: Optional[datetime] = None) -> bool:
    """True when this machine's ``[0, 1)`` score is below the current ramp."""
    try:
        if isinstance(score, bool) or not isinstance(score, (int, float)):
            return False
        if score < 0 or score >= 1:
            return False
        return score < _effective_fraction(rollout, now)
    except Exception:
        log.debug("Treating unreadable slice check as hold", exc_info=True)
        return False


def _parse_rollout(manifest_dict: object) -> Rollout:
    if not isinstance(manifest_dict, dict):
        return HOLD
    raw = manifest_dict.get("rollout")
    if raw is None:
        return HOLD
    if not isinstance(raw, dict):
        log.debug("Manifest rollout is not an object; treating as hold")
        return HOLD

    policy = _policy(raw.get("policy"))
    if policy is None:
        log.debug("Unknown or missing rollout policy; treating as hold")
        return HOLD
    if policy is RolloutPolicy.HOLD:
        return HOLD
    if policy is RolloutPolicy.IMMEDIATE:
        return IMMEDIATE

    released_at = _released_at(raw.get("released_at"))
    if released_at is None:
        log.debug("Invalid or missing staged released_at; treating as hold")
        return HOLD
    return Rollout(policy=RolloutPolicy.STAGED, released_at=released_at)


def _effective_fraction(rollout: Rollout, now: Optional[datetime]) -> float:
    if rollout.policy is RolloutPolicy.HOLD:
        return 0.0
    if rollout.policy is RolloutPolicy.IMMEDIATE:
        return 1.0
    if rollout.released_at is None:
        return 0.0
    current = _as_utc(now if now is not None else datetime.now(timezone.utc))
    if current is None:
        return 0.0
    elapsed = (current - rollout.released_at).total_seconds()
    if elapsed <= 0:
        return 0.0
    return min(1.0, elapsed / _STAGED_RAMP_SECONDS)


def _policy(value: object) -> Optional[RolloutPolicy]:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    try:
        return RolloutPolicy(normalized)
    except ValueError:
        return None


def _released_at(value: object) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> Optional[datetime]:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
