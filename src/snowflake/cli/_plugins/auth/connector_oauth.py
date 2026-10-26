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

Connector calls that differ between connector 4 and the Universal Driver live
in ``connector_adapter``. Both connectors cache a session under the user
string passed to ``connect()``, and they do it before Snowflake reports the
session user. When this user and account host have no cached session, login
connects twice. The first connection omits the user and does not cache. The
second passes the session user and caches. A cached session is left in place,
and the browser stays closed.

``host`` is included only when login will store that host, so a later command
builds the same cache key.
"""

from __future__ import annotations

import logging

import snowflake.connector
from snowflake.cli._plugins.auth.connector_adapter import (
    oauth_access_token,
    public_account_host,
    session_user_cache_key,
)
from snowflake.cli._plugins.auth.sign_in_target import SignInTarget
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
        """Whether ``connect(user=other)`` hits the entry stored for this user."""
        return session_user_cache_key(self.value) == session_user_cache_key(other.value)


def account_host(target: SignInTarget) -> str:
    """Account host the connector uses as the session cache key.

    A host given to login is that key. A bare account uses the public account
    host.
    """
    if target.host:
        return target.host
    return public_account_host(target.account)


def session_is_cached(*, user: str, account_host: str) -> bool:
    """Whether this user and account host already have a cached session.

    The connector opens the browser when the access token is missing. A lookup
    that fails is a miss, so login signs in instead of treating a broken cache
    as a session.
    """
    if not user.strip() or not account_host.strip():
        return False
    return oauth_access_token(user=user, host=account_host) is not None


def sign_in(
    *,
    target: SignInTarget,
    application: str,
    role: str | None = None,
    warehouse: str | None = None,
    database: str | None = None,
    schema: str | None = None,
) -> str:
    """Open the connector's browser or paste path and return the session user.

    The returned string is the user a later ``connect()`` must pass. The first
    attempt is not cached. A machine with no browser still finishes here: the
    connector asks for the redirect, and this function does not ask for a user.

    ``role`` is the connection role, passed through with no OAuth scope of our
    own. The connector may scope the new session to that role. A named
    warehouse, database, or schema is checked on the login request. Snowflake
    rejects one that does not exist or that this user cannot use.
    """
    discovered = SessionUser(
        _open_session(
            target,
            user=None,
            application=application,
            store=False,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
        )
    )
    confirmed = SessionUser(
        _open_session(
            target,
            user=discovered.value,
            application=application,
            store=True,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
        )
    )
    if not discovered.shares_cache_key_with(confirmed):
        raise CliConnectionError(
            f"Signed in as {confirmed.value}. "
            f"The first sign-in was {discovered.value}. "
            "Sign in as the same user and try again."
        )
    return discovered.value


def check_connection_context(
    *,
    target: SignInTarget,
    user: str,
    application: str,
    role: str | None = None,
    warehouse: str | None = None,
    database: str | None = None,
    schema: str | None = None,
) -> None:
    """Ask Snowflake to accept a warehouse, database, or schema.

    The saved user is passed so a cached session is reused and the browser
    stays closed. A rejection raises before the caller writes the connection.
    """
    _open_session(
        target,
        user=user,
        application=application,
        store=True,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )


def _open_session(
    target: SignInTarget,
    *,
    user: str | None,
    application: str,
    store: bool,
    role: str | None = None,
    warehouse: str | None = None,
    database: str | None = None,
    schema: str | None = None,
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
    if role:
        kwargs["role"] = role
    named_context = False
    if warehouse:
        kwargs["warehouse"] = warehouse
        named_context = True
    if database:
        kwargs["database"] = database
        named_context = True
    if schema:
        kwargs["schema"] = schema
        named_context = True
    if named_context:
        kwargs["validate_default_parameters"] = True
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
