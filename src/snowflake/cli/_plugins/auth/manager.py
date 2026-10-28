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

import secrets
from dataclasses import dataclass

import typer
from snowflake.cli._plugins.auth.connector_oauth import (
    AUTHENTICATOR,
    SessionUser,
    connector_reads_refresh_token_flag,
    local_oauth_scope,
    sign_in,
)
from snowflake.cli._plugins.auth.sign_in_target import SignInTarget
from snowflake.cli.api.config import (
    ConnectionConfig,
    add_connection_to_proper_file,
    get_all_connections,
    get_connections_file,
    get_default_connection_name,
    remove_connection_from_proper_file,
    set_config_value,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import CliArgumentError, CliError
from snowflake.cli.api.output.types import CollectionResult, MessageResult
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.utils.tty import is_tty_interactive


@dataclass(frozen=True)
class _ConnectionEdits:
    """Connection fields this login sets or clears.

    A passed role is sent on both connects and checked there. A stored role
    is sent on the caching connect, once the session user is known, and
    checked there. A named warehouse, database, or schema is checked with
    the login and then stored.
    """

    role: str | None = None
    warehouse: str | None = None
    database: str | None = None
    schema: str | None = None
    clear_role: bool = False
    clear_warehouse: bool = False
    clear_database: bool = False
    clear_schema: bool = False

    def apply(self, config: ConnectionConfig) -> None:
        if self.clear_role:
            config.role = None
        elif self.role is not None:
            config.role = self.role
        if self.clear_warehouse:
            config.warehouse = None
        elif self.warehouse is not None:
            config.warehouse = self.warehouse
        if self.clear_database:
            config.database = None
        elif self.database is not None:
            config.database = self.database
        if self.clear_schema:
            config.schema = None
        elif self.schema is not None:
            config.schema = self.schema

    def role_for_discovery(self) -> str | None:
        """Role for the first connect, before the session user is known.

        A passed role wins. Clearing the role mints with none. An omitted
        role waits until the session user is known, so another person's
        saved role is not sent.
        """
        if self.clear_role:
            return None
        return self.role

    def role_for_caching(self, saved: ConnectionConfig | None) -> str | None:
        """Role for the caching connect once the session user is known.

        A passed role wins. Clearing the role mints with none. Otherwise the
        role stored for this user is used, so a repeat sign-in keeps it.
        """
        if self.clear_role:
            return None
        if self.role is not None:
            return self.role
        if saved is None:
            return None
        return saved.role

    def context_to_check(self) -> dict[str, str]:
        """Warehouse, database, and schema this login named.

        Omitted fields are absent, so a repeat login does not recheck them.
        """
        context: dict[str, str] = {}
        if self.warehouse:
            context["warehouse"] = self.warehouse
        if self.database:
            context["database"] = self.database
        if self.schema:
            context["schema"] = self.schema
        return context


@dataclass(frozen=True)
class _NameChoice:
    """Where this login writes, and which saved connection it copies."""

    name: str
    source_name: str | None
    kept_name_note: str | None = None


@dataclass(frozen=True)
class _SessionUserAtAccount:
    """The session user at an account. One local OAuth connection per pair.

    The derived name uses the session user Snowflake returned. Connector 4
    uppercases the cache key and connector 5 lowercases it, so a name built
    from that key would differ between them. A host or a role can change
    without this becoming a different connection.
    """

    account: str
    session_user: str

    def derived_name(self) -> str:
        """Account and session user, so two people on one account get two names."""
        return f"{self.account}-{self.session_user}"

    def matches(self, config: ConnectionConfig) -> bool:
        """True when this saved connection is local OAuth for this user and account."""
        if not _is_local_oauth(config):
            return False
        if (config.account or "").casefold() != self.account.casefold():
            return False
        return _same_session_user(config.user, self.session_user)


_SUFFIX_ATTEMPTS = 8


class AuthManager:
    """Saved Snowflake OAuth connections, and the login that writes one."""

    def login(
        self,
        account: str | None,
        host: str | None,
        *,
        role: str | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        clear_role: bool = False,
        clear_warehouse: bool = False,
        clear_database: bool = False,
        clear_schema: bool = False,
        connection_name: str | None = None,
    ) -> MessageResult:
        """Sign in and make that Snowflake OAuth connection the default.

        The account is the only prompt. Login always signs in, including when
        a session for this account is already cached, so the person can choose
        a different user. A session user who already has a local OAuth
        connection updates that connection. A different session user gets a
        new one. A host flag or an account URL replaces the stored host. A
        bare account keeps a host already stored for this user. A new
        connection stores a host only when one already belongs to this user,
        or when the connection named for the account has one. The caching
        connect receives that host, because it is part of the OAuth cache
        key. An omitted role is sent on the caching connect when this user
        already has one stored. A passed role is sent on both connects. An
        omitted warehouse, database, or schema leaves the stored value. A
        new connection is named from the account and the session user, or
        from ``connection_name`` when one is passed.
        """
        # Imported here to avoid a cycle through the CLI app while plugins load.
        from snowflake.cli._app.telemetry import command_info

        requested_name = _explicit_connection_name(connection_name)
        _reject_name_owned_by_another_authenticator(requested_name)
        requested = SignInTarget.from_login(account, host)
        edits = _ConnectionEdits(
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            clear_role=clear_role,
            clear_warehouse=clear_warehouse,
            clear_database=clear_database,
            clear_schema=clear_schema,
        )
        saved = _local_oauth_for_account(requested.account)
        session_role: str | None = None

        def caching_session(user: str) -> tuple[SignInTarget, str | None]:
            nonlocal session_role
            cache_target, session_role = _caching_session(requested, edits, user)
            return cache_target, session_role

        session_user = sign_in(
            target=SignInTarget(
                requested.account,
                _host_for_discovery(requested, saved),
            ),
            application=command_info(),
            role=edits.role_for_discovery(),
            caching_session=caching_session,
            **edits.context_to_check(),
        )
        signed_in = _SessionUserAtAccount(requested.account, session_user)
        target = SignInTarget(
            requested.account,
            _host_to_store(requested, signed_in),
        )
        choice = _choose_connection_name(
            signed_in,
            existing_name=_existing_local_oauth_name(signed_in),
            requested_name=requested_name,
        )
        return _save_connection(
            choice.name,
            target,
            user=session_user,
            edits=edits,
            source_name=choice.source_name,
            kept_name_note=choice.kept_name_note,
            session_role=session_role,
        )

    def list_connections(self) -> CollectionResult:
        """Saved connections, including ones with no session."""
        connections_file = get_connections_file()
        if connections_file.exists():
            path = sanitize_for_terminal(str(connections_file))
            cli_console.warning(
                f"Reading connections from {path}. Entries from config.toml are ignored."
            )
        default_name = get_default_connection_name()
        rows = (
            {
                "connection_name": name,
                "account": config.account or "",
                "user": config.user or "",
                "host": config.host or "",
                "database": config.database or "",
                "schema": config.schema or "",
                "warehouse": config.warehouse or "",
                "role": config.role or "",
                "is_default": name == default_name,
            }
            for name, config in get_all_connections().items()
            if _is_local_oauth(config)
        )
        return CollectionResult(rows)


def _caching_session(
    requested: SignInTarget,
    edits: _ConnectionEdits,
    session_user: str,
) -> tuple[SignInTarget, str | None]:
    """Host and role for the connect that caches this session user."""
    signed_in = _SessionUserAtAccount(requested.account, session_user)
    saved = _saved_local_oauth(signed_in)
    config = None if saved is None else saved[1]
    return (
        SignInTarget(requested.account, _host_to_store(requested, signed_in)),
        edits.role_for_caching(config),
    )


def _host_for_discovery(
    requested: SignInTarget,
    saved: list[tuple[str, ConnectionConfig]],
) -> str | None:
    """Host for the first connect, before the session user is known.

    A host flag or an account URL is used. A bare account uses the host on
    the one saved local OAuth connection, so a private account still opens.
    Otherwise it uses the host on the connection whose name is the account.
    With no stored host, ``connect()`` omits it and the connector uses the
    public account host.
    """
    if requested.host:
        return requested.host
    if len(saved) == 1:
        return saved[0][1].host or None
    return _host_on_account_named_connection(requested.account)


def _host_to_store(
    requested: SignInTarget,
    signed_in: _SessionUserAtAccount,
) -> str | None:
    """Host this login writes, which the caching ``connect()`` must use.

    A host flag or an account URL replaces a stored host. A bare account
    keeps the host already stored for this user. A new connection takes a
    host from the connection whose name is the account, when that connection
    has one. Another person's local OAuth host is not copied. With no stored
    host, ``connect()`` omits it and the connector uses the public account
    host.
    """
    if requested.host:
        return requested.host
    saved = _saved_local_oauth(signed_in)
    if saved is not None:
        return saved[1].host or None
    return _host_on_account_named_connection(requested.account)


def _host_on_account_named_connection(account: str) -> str | None:
    existing = get_all_connections().get(account)
    if existing is None or not existing.host:
        return None
    return existing.host


def _save_connection(
    name: str,
    target: SignInTarget,
    *,
    user: str,
    edits: _ConnectionEdits,
    source_name: str | None,
    session_role: str | None,
    kept_name_note: str | None = None,
) -> MessageResult:
    written = add_connection_to_proper_file(
        name,
        _connection_to_write(
            source_name,
            target,
            user=user,
            edits=edits,
            session_role=session_role,
        ),
    )
    set_config_value(path=["default_connection_name"], value=name)
    if source_name is not None and source_name != name:
        remove_connection_from_proper_file(source_name)
    message = (
        f"Wrote connection {name} to {written} "
        f"and changed the default connection to {name}."
    )
    if kept_name_note:
        message = f"{kept_name_note} {message}"
    return MessageResult(message)


def _explicit_connection_name(connection_name: str | None) -> str | None:
    if connection_name is None:
        return None
    name = connection_name.strip()
    if not name:
        raise CliArgumentError("Enter a connection name, or omit --connection-name.")
    return name


def _reject_name_owned_by_another_authenticator(name: str | None) -> None:
    """Stop before the browser when the explicit name is not local OAuth."""
    if name is None:
        return
    owner = get_all_connections().get(name)
    if owner is None or _is_local_oauth(owner):
        return
    raise CliError(
        f"Connection {name} already exists and does not use Snowflake OAuth "
        "for local applications. Choose a different connection name."
    )


def _choose_connection_name(
    signed_in: _SessionUserAtAccount,
    *,
    existing_name: str | None,
    requested_name: str | None,
) -> _NameChoice:
    """The connection name this login writes.

    An explicit name that belongs to another connection raises and writes
    nothing. A derived name that is already taken gets a short suffix.
    """
    if requested_name is not None:
        return _choose_explicit_name(
            signed_in,
            existing_name=existing_name,
            requested_name=requested_name,
        )
    if existing_name is not None:
        return _NameChoice(name=existing_name, source_name=existing_name)
    derived = signed_in.derived_name()
    taken = set(get_all_connections())
    if derived not in taken:
        return _NameChoice(name=derived, source_name=None)
    return _NameChoice(
        name=_suffixed_connection_name(derived, taken),
        source_name=None,
    )


def _choose_explicit_name(
    signed_in: _SessionUserAtAccount,
    *,
    existing_name: str | None,
    requested_name: str,
) -> _NameChoice:
    owner = get_all_connections().get(requested_name)
    if owner is not None and requested_name != existing_name:
        if signed_in.matches(owner):
            return _NameChoice(name=requested_name, source_name=requested_name)
        raise CliError(
            f"Connection {requested_name} belongs to a different user or account. "
            "Choose a different connection name."
        )
    if existing_name is None or requested_name == existing_name:
        source = existing_name if requested_name == existing_name else None
        return _NameChoice(name=requested_name, source_name=source)
    return _rename_or_keep(existing_name, requested_name)


def _rename_or_keep(existing_name: str, requested_name: str) -> _NameChoice:
    """Ask before a rename. With no terminal, keep the original name."""
    if not is_tty_interactive():
        return _NameChoice(
            name=existing_name,
            source_name=existing_name,
            kept_name_note=(
                f"Kept connection name {existing_name}. "
                f"A terminal is required to rename it to {requested_name}."
            ),
        )
    if typer.confirm(
        f"Rename connection {existing_name} to {requested_name}?",
        default=False,
    ):
        return _NameChoice(name=requested_name, source_name=existing_name)
    return _NameChoice(name=existing_name, source_name=existing_name)


def _suffixed_connection_name(base: str, taken: set[str]) -> str:
    for _ in range(_SUFFIX_ATTEMPTS):
        candidate = f"{base}-{secrets.token_hex(2)}"
        if candidate not in taken:
            return candidate
    raise CliError(
        f"Connection name {base} is already taken. "
        "Pass --connection-name to choose another."
    )


def _existing_local_oauth_name(signed_in: _SessionUserAtAccount) -> str | None:
    """The one saved local OAuth connection for this user and account."""
    saved = _saved_local_oauth(signed_in)
    if saved is None:
        return None
    return saved[0]


def _saved_local_oauth(
    signed_in: _SessionUserAtAccount,
) -> tuple[str, ConnectionConfig] | None:
    """The one saved local OAuth connection for this user and account."""
    matched = [
        (name, config)
        for name, config in _local_oauth_for_account(signed_in.account)
        if signed_in.matches(config)
    ]
    if len(matched) == 1:
        return matched[0]
    return None


def _local_oauth_for_account(account: str) -> list[tuple[str, ConnectionConfig]]:
    wanted = account.casefold()
    return [
        (name, config)
        for name, config in get_all_connections().items()
        if _is_local_oauth(config) and (config.account or "").casefold() == wanted
    ]


def _same_session_user(stored: str | None, session_user: str) -> bool:
    if stored is None or not stored.strip():
        return False
    return SessionUser(stored).shares_cache_key_with(SessionUser(session_user))


def _connection_to_write(
    source_name: str | None,
    target: SignInTarget,
    *,
    user: str,
    edits: _ConnectionEdits,
    session_role: str | None,
) -> ConnectionConfig:
    """OAuth fields for this sign-in.

    A new connection omits every connection field this sign-in did not name.
    An existing connection keeps every field this sign-in does not name.
    ``source_name`` is that existing connection, including when the login
    renames it. Login always rewrites the local OAuth scope, the refresh-token
    flag, and credential caching. It does not keep a previous value.
    """
    existing = None if source_name is None else get_all_connections().get(source_name)
    if existing is None:
        config = ConnectionConfig(
            account=target.account,
            user=user,
            host=target.host,
            authenticator=AUTHENTICATOR,
        )
    else:
        config = ConnectionConfig.from_dict(existing.to_dict_of_all_non_empty_values())
        config.account = target.account
        config.user = user
        config.authenticator = AUTHENTICATOR
        if target.host:
            config.host = target.host
    edits.apply(config)
    config.oauth_scope = local_oauth_scope(session_role)
    config.client_store_temporary_credential = True
    if connector_reads_refresh_token_flag():
        config.oauth_enable_refresh_tokens = True
    else:
        config.oauth_enable_refresh_tokens = None
    return config


def _is_local_oauth(config: ConnectionConfig) -> bool:
    """Snowflake OAuth for local applications, which uses the built-in client."""
    if (config.authenticator or "").upper() != AUTHENTICATOR:
        return False
    return not config.oauth_client_id and not config.oauth_client_secret
