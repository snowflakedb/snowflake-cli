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

import json
import logging
import math
import time
from typing import Optional

from snowflake.cli._plugins.feedback.constants import (
    RATE_LIMIT_MAX,
    RATE_LIMIT_WINDOW_SECONDS,
)
from snowflake.cli.api.config import get_config_manager
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

_RATE_FILE_NAME = "feedback_rate.json"


def _rate_file() -> SecurePath:
    return SecurePath(get_config_manager().file_path.parent / _RATE_FILE_NAME)


def _read_timestamps(path: SecurePath) -> list[float]:
    if not path.exists():
        return []
    try:
        payload = json.loads(
            path.read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding="utf-8")
        )
    except Exception:
        log.debug("Ignoring unreadable feedback rate-limit file", exc_info=True)
        return []
    timestamps = payload.get("timestamps") if isinstance(payload, dict) else payload
    if not isinstance(timestamps, list):
        return []
    return [float(ts) for ts in timestamps if isinstance(ts, (int, float))]


def _recent_timestamps(now: float) -> list[float]:
    cutoff = now - RATE_LIMIT_WINDOW_SECONDS
    return [ts for ts in _read_timestamps(_rate_file()) if ts > cutoff]


def check(now: Optional[float] = None) -> Optional[int]:
    """Return retry_after_seconds when the rolling-hour cap is already hit."""
    now = time.time() if now is None else now
    recent = _recent_timestamps(now)
    if len(recent) >= RATE_LIMIT_MAX:
        retry_after = math.ceil(min(recent) + RATE_LIMIT_WINDOW_SECONDS - now)
        return max(retry_after, 1)
    return None


def record(now: Optional[float] = None) -> None:
    """Persist a successful submission. Call only after send succeeds."""
    now = time.time() if now is None else now
    recent = _recent_timestamps(now)
    recent.append(now)
    path = _rate_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"timestamps": recent}),
            encoding="utf-8",
        )
    except OSError:
        log.debug("Failed to persist feedback rate-limit file", exc_info=True)
