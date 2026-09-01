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

"""Compatibility helpers for connector exceptions across driver versions.

The CLI runs against both the legacy ``snowflake-connector-python`` v4.x and
the Universal Driver v5. ``SnowflakeRestful.fetch(..., raise_raw_http_failure=True)``
reports HTTP failures differently between the two:

* **v4.x** raises ``snowflake.connector.vendored.requests.exceptions.HTTPError``
  (the vendored ``requests`` package), with the status on ``err.response.status_code``.
  The ``vendored`` sub-package does not exist in v5.
* **v5** raises ``snowflake.connector.errors.OperationalError`` with the HTTP
  status on a dedicated ``http_status`` attribute.

This module hides that difference behind :data:`HTTP_FAILURE_ERRORS` (the
exception classes to catch) and :func:`get_http_status_code` (extract the status).
"""

from __future__ import annotations

from typing import Optional, Tuple, Type

from snowflake.connector.version import VERSION

#: True when running against the Universal Driver (connector major >= 5).
IS_V5_DRIVER = VERSION[0] >= 5

HTTP_FAILURE_ERRORS: Tuple[Type[BaseException], ...]

if IS_V5_DRIVER:
    from snowflake.connector.errors import OperationalError

    # OperationalError is broad; only instances carrying an ``http_status`` are
    # raw HTTP failures (see get_http_status_code). Others are re-raised by callers.
    HTTP_FAILURE_ERRORS = (OperationalError,)
else:
    from snowflake.connector.vendored.requests.exceptions import HTTPError

    HTTP_FAILURE_ERRORS = (HTTPError,)


def get_http_status_code(err: BaseException) -> Optional[int]:
    """Return the HTTP status of a raw HTTP failure, or None if it is not one."""
    if IS_V5_DRIVER:
        # Universal Driver v5: OperationalError carries the status on http_status.
        status = getattr(err, "http_status", None)
    else:
        # connector v4: vendored HTTPError exposes .response.status_code.
        response = getattr(err, "response", None)
        status = getattr(response, "status_code", None)
    return int(status) if status is not None else None


def get_user_agent(rest) -> Optional[str]:
    """Return the User-Agent to advertise on REST requests, or None.

    On the Universal Driver v5 the value is built by the core and read via
    ``SnowflakeRestful.get_user_agent()``; on connector v4.x it is the
    ``PYTHON_CONNECTOR_USER_AGENT`` constant from ``snowflake.connector.network``.
    """
    if IS_V5_DRIVER:
        return rest.get_user_agent()
    from snowflake.connector.network import PYTHON_CONNECTOR_USER_AGENT

    return PYTHON_CONNECTOR_USER_AGENT
