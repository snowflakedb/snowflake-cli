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
session user. Login always connects twice, including when a session is
already cached. The first connection omits the user, so the browser or paste
path can choose who is signing in, and does not cache. The second passes the
session user and caches. A later command reuses that session.

``host`` on the caching connect is the host login will store, so a later
command builds the same cache key. The first connect can use a different
host. ``caching_session`` supplies the host and role for the second connect
once the session user is known. When it is omitted, both connects use the
same host and role.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import snowflake.connector
from packaging.version import Version
from snowflake.cli._plugins.auth.connector_adapter import session_user_cache_key
from snowflake.cli._plugins.auth.sign_in_target import SignInTarget
from snowflake.cli.api.exceptions import CliConnectionError
from snowflake.connector.errors import Error as SnowflakeError

log = logging.getLogger(__name__)

# Snowflake OAuth for local applications uses the built-in client.
AUTHENTICATOR = "OAUTH_AUTHORIZATION_CODE"
_REFRESH_TOKEN_SCOPE = "refresh_token"


def connector_reads_refresh_token_flag() -> bool:
    """Whether this connector still reads ``oauth_enable_refresh_tokens``.

    Connector 5 warns and ignores the key, so login leaves it unset. The
    choice is made when login writes the connection. A later command sends
    the stored value and does not ask again.
    """
    return Version(snowflake.connector.__version__).major < 5


def local_oauth_scope(role: str | None) -> str:
    """Scope one login connect sends for the role that connect receives.

    The role spelling is the one passed to ``connect()``. No role keeps the
    scope to ``refresh_token``, with no ``session:role``.
    """
    if role:
        return f"{_REFRESH_TOKEN_SCOPE} session:role:{role}"
    return _REFRESH_TOKEN_SCOPE


class SessionUser:
    """The user ``CURRENT_USER()`` returned, in the spelling the connector caches."""

    def __init__(self, returned: str):
        self.value = returned.strip()

    def shares_cache_key_with(self, other: SessionUser) -> bool:
        """Whether ``connect(user=other)`` hits the entry stored for this user."""
        return session_user_cache_key(self.value) == session_user_cache_key(other.value)


def sign_in(
    *,
    target: SignInTarget,
    application: str,
    role: str | None = None,
    warehouse: str | None = None,
    database: str | None = None,
    schema: str | None = None,
    caching_session: Callable[[str], tuple[SignInTarget, str | None]] | None = None,
) -> str:
    """Open the connector's browser or paste path and return the session user.

    The returned string is the user a later ``connect()`` must pass. The first
    attempt is not cached. A machine with no browser still finishes here: the
    connector asks for the redirect, and this function does not ask for a user.

    ``role`` is the role for the first connect. Each connect sends
    ``local_oauth_scope`` for the role that connect receives.
    ``caching_session``, when given, receives the session user and returns
    the target and role for the connect that caches. A role, warehouse,
    database, or schema is checked on the login request that names it.
    Snowflake rejects one that does not exist or that this user cannot use.
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
    cache_target = target
    cache_role = role
    if caching_session is not None:
        cache_target, cache_role = caching_session(discovered.value)
    confirmed = SessionUser(
        _open_session(
            cache_target,
            user=discovered.value,
            application=application,
            store=True,
            role=cache_role,
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
        "oauth_scope": local_oauth_scope(role),
    }
    if connector_reads_refresh_token_flag():
        kwargs["oauth_enable_refresh_tokens"] = True
    if user is not None:
        kwargs["user"] = user
    if target.host:
        kwargs["host"] = target.host
    if role:
        kwargs["role"] = role
        kwargs["validate_default_parameters"] = True
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
