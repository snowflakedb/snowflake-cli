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

    A passed role is sent on both connects and checked there. An omitted
    role sends none, including when this user already has a role stored on
    another connection. A named warehouse, database, or schema is checked
    with the login and then stored.
    """

    role: str | None = None
    warehouse: str | None = None
    database: str | None = None
    schema: str | None = None
    clear_warehouse: bool = False
    clear_database: bool = False
    clear_schema: bool = False

    def apply(self, config: ConnectionConfig) -> None:
        if self.role is not None:
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
    """One local OAuth connection: account, session user, and connection role.

    The derived name uses the session user Snowflake returned. Connector 4
    uppercases the cache key and connector 5 lowercases it, so a name built
    from that key would differ between them. No role is its own connection.
    A different role is a different connection. A host can change without
    this becoming a different connection.
    """

    account: str
    session_user: str
    role: str | None = None

    def derived_name(self) -> str:
        """Account, session user, and the role when this login passed one."""
        base = f"{self.account}-{self.session_user}"
        if self.role is None:
            return base
        return f"{base}-{self.role}"

    def matches(self, config: ConnectionConfig) -> bool:
        """True when this saved connection is local OAuth for this key."""
        if not _is_local_oauth(config):
            return False
        if (config.account or "").casefold() != self.account.casefold():
            return False
        if not _same_session_user(config.user, self.session_user):
            return False
        return _same_connection_role(config.role, self.role)


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
        clear_warehouse: bool = False,
        clear_database: bool = False,
        clear_schema: bool = False,
        connection_name: str | None = None,
    ) -> MessageResult:
        """Sign in and make that Snowflake OAuth connection the default.

        The account is the only prompt. Login always signs in, including when
        a session for this account is already cached, so the person can choose
        a different user. One local OAuth connection per account, session
        user, and connection role. A matching login updates that connection.
        A different role writes a new one and leaves the others alone. No
        role is its own connection. A login that omits the role sends no role
        on either connect. A host flag or an account URL replaces the stored
        host. A bare account uses a host every local OAuth connection for
        that account shares, then the host on the connection named for the
        account. A new connection stores a host shared by this session user's
        connections for the account, then the account-named host. It stores
        only the warehouse, database,
        and schema this login names. A new connection is named from the
        account, the session user, and the role when one is passed, or from
        ``connection_name`` when one is passed.
        """
        # Imported here to avoid a cycle through the CLI app while plugins load.
        from snowflake.cli._app.telemetry import command_info

        role = _explicit_role(role)
        requested_name = _explicit_connection_name(connection_name)
        _reject_name_owned_by_another_authenticator(requested_name)
        requested = SignInTarget.from_login(account, host)
        edits = _ConnectionEdits(
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            clear_warehouse=clear_warehouse,
            clear_database=clear_database,
            clear_schema=clear_schema,
        )
        saved = _local_oauth_for_account(requested.account)
        session_role: str | None = None

        def caching_session(user: str) -> tuple[SignInTarget, str | None]:
            nonlocal session_role
            cache_target, session_role = _caching_session(requested, edits.role, user)
            return cache_target, session_role

        signed = sign_in(
            target=SignInTarget(
                requested.account,
                _host_for_discovery(requested, saved),
            ),
            application=command_info(),
            role=edits.role,
            caching_session=caching_session,
            **edits.context_to_check(),
        )
        # The caching connection stays open so the command's telemetry can
        # leave on it. Close it here when login does not finish.
        held = False
        try:
            session_user = signed.user
            signed_in = _SessionUserAtAccount(requested.account, session_user, role)
            target = SignInTarget(
                requested.account,
                _host_to_store(requested, signed_in),
            )
            choice = _choose_connection_name(
                signed_in,
                existing_name=_existing_local_oauth_name(signed_in),
                requested_name=requested_name,
            )
            result = _save_connection(
                choice.name,
                target,
                user=session_user,
                edits=edits,
                source_name=choice.source_name,
                kept_name_note=choice.kept_name_note,
                session_role=session_role,
            )
            from snowflake.cli._app.telemetry import hold_login_connection

            hold_login_connection(signed.connection, signed.parameters)
            held = True
            return result
        finally:
            if not held:
                try:
                    signed.connection.close()
                except Exception:
                    pass

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
    role: str | None,
    session_user: str,
) -> tuple[SignInTarget, str | None]:
    """Host and role for the connect that caches this session user.

    The role is the one this login passed. An omitted role is not filled in
    from a saved connection.
    """
    signed_in = _SessionUserAtAccount(requested.account, session_user, role)
    return (
        SignInTarget(requested.account, _host_to_store(requested, signed_in)),
        role,
    )


def _host_for_discovery(
    requested: SignInTarget,
    saved: list[tuple[str, ConnectionConfig]],
) -> str | None:
    """Host for the first connect, before the session user is known.

    A host flag or an account URL is used. A bare account uses the host when
    every saved local OAuth connection for that account stores that same
    host. Otherwise it uses the host on the connection whose name is the
    account. With no stored host, ``connect()`` omits it and the connector
    uses the public account host.
    """
    return _shared_or_account_named_host(requested, saved)


def _host_to_store(
    requested: SignInTarget,
    signed_in: _SessionUserAtAccount,
) -> str | None:
    """Host this login writes, which the caching ``connect()`` must use.

    A host flag or an account URL replaces a stored host. A matching
    connection keeps the host it already stores. A new connection takes a
    host every local OAuth connection this session user has for the account
    shares, then the host on the connection whose name is the account.
    Another person's local OAuth host is not copied. With no stored host,
    ``connect()`` omits it and the connector uses the public account host.
    """
    if requested.host:
        return requested.host
    saved = _saved_local_oauth(signed_in)
    if saved is not None:
        return saved[1].host or None
    return _shared_or_account_named_host(
        requested,
        _local_oauth_for_session_user(signed_in),
    )


def _shared_or_account_named_host(
    requested: SignInTarget,
    saved: list[tuple[str, ConnectionConfig]],
) -> str | None:
    """Explicit host, a host every connection stores, or the account-named host."""
    if requested.host:
        return requested.host
    shared = _shared_host(saved)
    if shared is not None:
        return shared
    return _host_on_account_named_connection(requested.account)


def _shared_host(saved: list[tuple[str, ConnectionConfig]]) -> str | None:
    """The host when every connection stores that same host."""
    if not saved:
        return None
    hosts = [config.host or None for _, config in saved]
    first = hosts[0]
    if first and all(host == first for host in hosts):
        return first
    return None


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


def _explicit_role(role: str | None) -> str | None:
    if role is None:
        return None
    if not role.strip():
        raise CliArgumentError("Role is blank. Enter a role, or omit --role.")
    return role


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
            f"Connection {requested_name} belongs to a different user, account, or role. "
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
    """The saved local OAuth connection for this account, user, and role."""
    saved = _saved_local_oauth(signed_in)
    if saved is None:
        return None
    return saved[0]


def _saved_local_oauth(
    signed_in: _SessionUserAtAccount,
) -> tuple[str, ConnectionConfig] | None:
    """The saved local OAuth connection for this account, user, and role."""
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


def _local_oauth_for_session_user(
    signed_in: _SessionUserAtAccount,
) -> list[tuple[str, ConnectionConfig]]:
    """Local OAuth connections this session user has for the account, any role."""
    return [
        (name, config)
        for name, config in _local_oauth_for_account(signed_in.account)
        if _same_session_user(config.user, signed_in.session_user)
    ]


def _same_session_user(stored: str | None, session_user: str) -> bool:
    if stored is None or not stored.strip():
        return False
    return SessionUser(stored).shares_cache_key_with(SessionUser(session_user))


def _same_connection_role(stored: str | None, given: str | None) -> bool:
    """Unquoted roles compare case-insensitively. A quoted role matches only itself.

    No role is its own key. A quoted spelling and an unquoted spelling stay
    different connections, even when Snowflake would treat them as one role.
    """
    stored_blank = stored is None or not stored.strip()
    given_blank = given is None or not given.strip()
    if stored_blank or given_blank:
        return stored_blank and given_blank
    if stored is None or given is None:
        return False
    if _quoted_role(stored) or _quoted_role(given):
        return stored == given
    return stored.casefold() == given.casefold()


def _quoted_role(role: str) -> bool:
    return len(role) >= 2 and role.startswith('"') and role.endswith('"')


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
    """Snowflake OAuth for local applications, which uses the built-in client.

    A client id or secret of ``LOCAL_APPLICATION`` still counts as a custom
    client here. Command telemetry classifies that literal as the built-in
    client, ``oauth_local_application``. The two checks are intentionally
    different.
    """
    if (config.authenticator or "").upper() != AUTHENTICATOR:
        return False
    return not config.oauth_client_id and not config.oauth_client_secret
