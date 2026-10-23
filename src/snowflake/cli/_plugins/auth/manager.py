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

from snowflake.cli._plugins.auth.connector_oauth import AUTHENTICATOR, sign_in
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


class AuthManager:
    """Saved Snowflake OAuth connections, and the login that writes one."""

    def login(self, account: str | None, host: str | None) -> MessageResult:
        """Sign in and make that Snowflake OAuth connection the default.

        The account is the only input. A host flag or an account URL replaces
        the stored host. A bare account keeps a host already stored for that
        connection, and a new connection stores none. Database and schema are
        not parameters of this sign-in. The connector receives the host this
        login will store, because that host is part of the OAuth cache key.
        """
        # Imported here to avoid a cycle through the CLI app while plugins load.
        from snowflake.cli._app.telemetry import command_info

        requested = SignInTarget.from_login(account, host)
        name = requested.account
        target = SignInTarget(name, _host_this_login_stores(name, requested))
        session_user = sign_in(target=target, application=command_info())
        written = add_connection_to_proper_file(
            name,
            _connection_to_write(name, target, user=session_user),
        )
        set_config_value(path=["default_connection_name"], value=name)
        return MessageResult(
            f"Wrote connection {name} to {written} "
            f"and changed the default connection to {name}."
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


def _host_this_login_stores(name: str, target: SignInTarget) -> str | None:
    """The host this login writes, which ``connect()`` must use.

    A host flag or an account URL replaces a stored host. A bare account
    keeps the host already stored. With no stored host, ``connect()`` omits
    it and the connector uses the public account host.
    """
    if target.host:
        return target.host
    existing = get_all_connections().get(name)
    if existing is None or not existing.host:
        return None
    return existing.host


def _connection_to_write(
    name: str,
    target: SignInTarget,
    *,
    user: str,
) -> ConnectionConfig:
    """OAuth fields for this sign-in, leaving database and schema in place.

    A new connection omits them. An existing connection keeps every field this
    sign-in does not name, including database and schema.
    """
    existing = get_all_connections().get(name)
    if existing is None:
        return ConnectionConfig(
            account=target.account,
            user=user,
            host=target.host,
            authenticator=AUTHENTICATOR,
            client_store_temporary_credential=True,
        )
    config = ConnectionConfig.from_dict(existing.to_dict_of_all_non_empty_values())
    config.account = target.account
    config.user = user
    config.authenticator = AUTHENTICATOR
    config.client_store_temporary_credential = True
    if target.host:
        config.host = target.host
    return config


def _is_local_oauth(config: ConnectionConfig) -> bool:
    """Snowflake OAuth for local applications, which uses the built-in client."""
    if (config.authenticator or "").upper() != AUTHENTICATOR:
        return False
    return not config.oauth_client_id and not config.oauth_client_secret
