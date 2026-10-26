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

"""Connector calls that differ between connector 4 and the Universal Driver.

Login goes through this module for every connector API that is not the same
on both. A move to connector 5 deletes the connector 4 branch in each function.
"""

from __future__ import annotations

import logging

from snowflake.cli.api.connector_errors import IS_V5_DRIVER
from snowflake.connector import util_text

log = logging.getLogger(__name__)


def public_account_host(account: str) -> str:
    """Public host for a bare account.

    Connector 4 builds it with ``construct_hostname``. Connector 5 removed
    that helper and appends ``.snowflakecomputing.com``.
    """
    construct = getattr(util_text, "construct_hostname", None)
    if construct is not None:
        return construct(None, account)
    return f"{account}.snowflakecomputing.com"


def session_user_cache_key(name: str) -> str:
    """Spelling this connector stores for the session user.

    Connector 4 uppercases the user. Connector 5 keeps a name that contains
    a double quote as written and lowercases every other name.
    """
    if IS_V5_DRIVER:
        if '"' in name:
            return name
        return name.lower()
    return name.upper()


def oauth_access_token(*, user: str, host: str) -> str | None:
    """Cached OAuth access token, or None when this connector cannot be read.

    Connector 4 stores the token in ``TokenCache``. Connector 5 has no Python
    cache API, so a lookup here is a miss. A lookup that fails is also a miss.
    """
    try:
        # Connector 5 does not ship this module.
        from snowflake.connector.token_cache import TokenCache, TokenKey, TokenType

        cache = TokenCache.make()
        if cache is None:
            return None
        token = cache.retrieve(
            TokenKey(
                user=user,
                host=host,
                tokenType=TokenType.OAUTH_ACCESS_TOKEN,
            )
        )
    except Exception:
        log.debug(
            "Could not read the session cache for account host %s",
            host,
            exc_info=True,
        )
        return None
    if isinstance(token, str) and token.strip():
        return token
    return None
