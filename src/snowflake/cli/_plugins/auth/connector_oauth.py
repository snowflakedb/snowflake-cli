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

"""Local OAuth sign-in on the public connector API.

Both the 4.x connector and the Universal Driver cache a session under the
user string passed to ``connect()``, and they do it before Snowflake reports
the session user. Login therefore connects twice. The first connection omits
the user and does not cache. The second passes the session user and caches.
Callers pass the same arguments on either connector.

``host`` is included only when login will store that host, so a later command
builds the same cache key.
"""

from __future__ import annotations

import logging

import snowflake.connector
from snowflake.cli._plugins.auth.sign_in_target import SignInTarget
from snowflake.cli.api.connector_errors import IS_V5_DRIVER
from snowflake.cli.api.exceptions import CliConnectionError
from snowflake.connector.errors import Error as SnowflakeError

log = logging.getLogger(__name__)

# Snowflake OAuth for local applications uses the built-in client.
AUTHENTICATOR = "OAUTH_AUTHORIZATION_CODE"


class SessionUser:
    """The user ``CURRENT_USER()`` returned, in the spelling the connector caches."""

    def __init__(self, returned: str):
        self.value = returned.strip()

    def shares_cache_key_with(self, other: SessionUser) -> bool:
        """Whether ``connect(user=other)`` hits the entry stored for this user.

        Connector 4 uppercases the user. Connector 5 keeps a name that contains
        a double quote as written and lowercases every other name.
        """
        return _cache_key(self.value) == _cache_key(other.value)


def _cache_key(name: str) -> str:
    if IS_V5_DRIVER:
        if _v5_keeps_spelling(name):
            return name
        return name.lower()
    return name.upper()


def _v5_keeps_spelling(name: str) -> bool:
    """Connector 5 stores a user string that contains a double quote as written."""
    return '"' in name


def sign_in(*, target: SignInTarget, application: str) -> str:
    """Open the connector's browser or paste path and return the session user.

    The returned string is the user a later ``connect()`` must pass. The first
    attempt is not cached. A machine with no browser still finishes here: the
    connector asks for the redirect, and this function does not ask for a user.
    """
    discovered = SessionUser(
        _open_session(target, user=None, application=application, store=False)
    )
    confirmed = SessionUser(
        _open_session(
            target,
            user=discovered.value,
            application=application,
            store=True,
        )
    )
    if not discovered.shares_cache_key_with(confirmed):
        raise CliConnectionError(
            f"Signed in as {confirmed.value}. "
            f"The first sign-in was {discovered.value}. "
            "Sign in as the same user and try again."
        )
    return discovered.value


def _open_session(
    target: SignInTarget,
    *,
    user: str | None,
    application: str,
    store: bool,
) -> str:
    kwargs: dict[str, str | bool] = {
        "account": target.account,
        "authenticator": AUTHENTICATOR,
        "application": application,
        "client_store_temporary_credential": store,
    }
    if user is not None:
        kwargs["user"] = user
    if target.host:
        kwargs["host"] = target.host
    log.debug(
        "Signing in to account %s as %s (store=%s)",
        target.account,
        user or "",
        store,
    )
    try:
        with snowflake.connector.connect(**kwargs) as connection:
            return _session_user(connection)
    except CliConnectionError:
        raise
    except SnowflakeError as error:
        log.debug("Sign-in failed for account %s", target.account, exc_info=error)
        detail = str(error).strip().rstrip(".")
        raise CliConnectionError(
            f"Sign-in failed: {detail}. Check the account and try again."
        ) from error


def _session_user(connection) -> str:
    with connection.cursor() as cursor:
        cursor.execute("SELECT CURRENT_USER()")
        row = cursor.fetchone()
    user = row[0] if row else None
    if not isinstance(user, str) or not user.strip():
        raise CliConnectionError("Sign-in did not return a user. Try again.")
    return user.strip()
