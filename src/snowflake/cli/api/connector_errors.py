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
exception classes to catch) and :func:`http_status_code` (extract the status).
"""

from __future__ import annotations

from typing import Optional, Tuple, Type

import snowflake.connector


def _connector_major() -> int:
    try:
        return int(snowflake.connector.__version__.split(".", 1)[0])
    except (ValueError, AttributeError):
        return 5  # assume the new driver if the version is unparseable


CONNECTOR_MAJOR = _connector_major()

HTTP_FAILURE_ERRORS: Tuple[Type[BaseException], ...]

if CONNECTOR_MAJOR >= 5:
    from snowflake.connector.errors import OperationalError

    # OperationalError is broad; only instances carrying an ``http_status`` are
    # raw HTTP failures (see http_status_code). Others are re-raised by callers.
    HTTP_FAILURE_ERRORS = (OperationalError,)
else:
    from snowflake.connector.vendored.requests.exceptions import HTTPError

    HTTP_FAILURE_ERRORS = (HTTPError,)


def http_status_code(err: BaseException) -> Optional[int]:
    """Return the HTTP status of a raw HTTP failure, or None if it is not one.

    Works across connector v4 (vendored ``HTTPError`` with ``.response``) and
    Universal Driver v5 (``OperationalError`` with ``.http_status``).
    """
    # Universal Driver v5: structured attribute set by SnowflakeRestful.
    status = getattr(err, "http_status", None)
    if status is not None:
        return int(status)
    # connector v4: vendored HTTPError exposes .response.status_code.
    response = getattr(err, "response", None)
    status = getattr(response, "status_code", None)
    return int(status) if status is not None else None
