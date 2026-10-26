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

from dataclasses import dataclass

from snowflake.cli._plugins.auth import connector_oauth
from snowflake.cli._plugins.auth.connector_oauth import (
    AUTHENTICATOR,
    SessionUser,
    sign_in,
)
from snowflake.cli._plugins.auth.sign_in_target import SignInTarget
from snowflake.cli.api.config import (
    ConnectionConfig,
    add_connection_to_proper_file,
    get_all_connections,
    get_connections_file,
    get_default_connection_name,
    set_config_value,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.output.types import CollectionResult, MessageResult
from snowflake.cli.api.sanitizers import sanitize_for_terminal


@dataclass(frozen=True)
class _ConnectionEdits:
    """Connection fields this login sets or clears.

    Role is passed when a session is minted. A named warehouse, database, or
    schema is checked with the login and then stored on the connection.
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

    def role_for_session(self, saved: list[tuple[str, ConnectionConfig]]) -> str | None:
        """Connection role to pass when a session is minted.

        A passed role wins. Clearing the role mints with none. Otherwise the
        one saved connection's role is used, so a repeat sign-in keeps that role.
        """
        if self.clear_role:
            return None
        if self.role is not None:
            return self.role
        if len(saved) == 1:
            config = saved[0][1]
            return config.role
        return None

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
    ) -> MessageResult:
        """Sign in and make that Snowflake OAuth connection the default.

        The account is the only prompt. A host flag or an account URL replaces
        the stored host. A bare account keeps a host already stored for that
        connection, and a new connection stores none. The connector receives
        the host this login will store, because that host is part of the OAuth
        cache key. An omitted connection role, warehouse, database, or schema
        leaves the stored value.
        """
        # Imported here to avoid a cycle through the CLI app while plugins load.
        from snowflake.cli._app.telemetry import command_info

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
        target = SignInTarget(
            requested.account,
            _host_this_login_stores(requested, saved),
        )
        if len(saved) == 1:
            name, config = saved[0]
            if config.user and connector_oauth.session_is_cached(
                user=config.user,
                account_host=connector_oauth.account_host(target),
            ):
                context = edits.context_to_check()
                if context:
                    connector_oauth.check_connection_context(
                        target=target,
                        user=config.user,
                        application=command_info(),
                        role=edits.role_for_session(saved),
                        **context,
                    )
                return _save_connection(name, target, user=config.user, edits=edits)

        session_user = sign_in(
            target=target,
            application=command_info(),
            role=edits.role_for_session(saved),
            **edits.context_to_check(),
        )
        name = _connection_name(target.account, session_user)
        return _save_connection(name, target, user=session_user, edits=edits)

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


def _host_this_login_stores(
    requested: SignInTarget,
    saved: list[tuple[str, ConnectionConfig]],
) -> str | None:
    """The host this login writes, which ``connect()`` must use.

    A host flag or an account URL replaces a stored host. A bare account
    keeps the host on the one saved local OAuth connection, or on the
    account-named connection when this login is not updating that one
    connection. With no stored host, ``connect()`` omits it and the connector
    uses the public account host.
    """
    if requested.host:
        return requested.host
    if len(saved) == 1:
        return saved[0][1].host or None
    existing = get_all_connections().get(requested.account)
    if existing is None or not existing.host:
        return None
    return existing.host


def _save_connection(
    name: str,
    target: SignInTarget,
    *,
    user: str,
    edits: _ConnectionEdits,
) -> MessageResult:
    written = add_connection_to_proper_file(
        name,
        _connection_to_write(name, target, user=user, edits=edits),
    )
    set_config_value(path=["default_connection_name"], value=name)
    return MessageResult(
        f"Wrote connection {name} to {written} "
        f"and changed the default connection to {name}."
    )


def _connection_name(account: str, session_user: str) -> str:
    """The saved local OAuth connection for this user and account, if there is one.

    A second login updates that connection. Anything else is named after the
    account, which is what a first login writes.
    """
    matched = [
        name
        for name, config in _local_oauth_for_account(account)
        if _same_session_user(config.user, session_user)
    ]
    if len(matched) == 1:
        return matched[0]
    return account


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
    name: str,
    target: SignInTarget,
    *,
    user: str,
    edits: _ConnectionEdits,
) -> ConnectionConfig:
    """OAuth fields for this sign-in.

    A new connection omits every connection field this sign-in did not name.
    An existing connection keeps every field this sign-in does not name.
    """
    existing = get_all_connections().get(name)
    if existing is None:
        config = ConnectionConfig(
            account=target.account,
            user=user,
            host=target.host,
            authenticator=AUTHENTICATOR,
            client_store_temporary_credential=True,
        )
    else:
        config = ConnectionConfig.from_dict(existing.to_dict_of_all_non_empty_values())
        config.account = target.account
        config.user = user
        config.authenticator = AUTHENTICATOR
        config.client_store_temporary_credential = True
        if target.host:
            config.host = target.host
    edits.apply(config)
    return config


def _is_local_oauth(config: ConnectionConfig) -> bool:
    """Snowflake OAuth for local applications, which uses the built-in client."""
    if (config.authenticator or "").upper() != AUTHENTICATOR:
        return False
    return not config.oauth_client_id and not config.oauth_client_secret
