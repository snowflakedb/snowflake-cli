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

import logging
import time

from snowflake import connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import OperationalError

# Session-scoped snowflake_session caches a failed connect for every test on
# that xdist worker. Deflake cannot recover those ERRORs. Retry login timeout
# here, before the fixture fails. Each attempt can block for login_timeout (120s).
LOGIN_TIMEOUT_MAX_RETRIES = 2
LOGIN_TIMEOUT_BACKOFF_START_S = 5
LOGIN_TIMEOUT_BACKOFF_CAP_S = 20

_sleep = time.sleep
_log = logging.getLogger(__name__)


def is_login_timeout_error(exc: BaseException) -> bool:
    """True when `exc` is a JWT/GS login timeout (HYT00 / 000014)."""
    if getattr(exc, "sqlstate", None) == "HYT00":
        return True
    text = str(exc).lower()
    return "login timed out" in text or "hyt00" in text


def connect_with_login_retry(**config) -> SnowflakeConnection:
    """Call `connector.connect`, retrying known login-timeout flakes."""
    backoff = LOGIN_TIMEOUT_BACKOFF_START_S
    attempts = LOGIN_TIMEOUT_MAX_RETRIES + 1
    for attempt in range(attempts):
        try:
            return connector.connect(**config)
        except OperationalError as exc:
            if not is_login_timeout_error(exc) or attempt == attempts - 1:
                raise
            _log.warning(
                "Snowflake login timed out, retrying in %ss (attempt %s/%s)",
                backoff,
                attempt + 1,
                attempts,
            )
            _sleep(backoff)
            backoff = min(backoff * 2, LOGIN_TIMEOUT_BACKOFF_CAP_S)
    raise AssertionError("unreachable")
