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

import asyncio
import logging
import re
import warnings
from pathlib import Path
from typing import Optional

from snowflake.cli.api.config import get_default_connection_name
from snowflake.cli.api.exceptions import InvalidSchemaError
from snowflake.connector import SnowflakeConnection
from snowflake.connector.compat import IS_WINDOWS

logger = logging.getLogger(__name__)

schema_pattern = re.compile(r".+\..+")


class ConnectionContext:
    # TODO: reduce duplication / boilerplate by using config.ConnectionConfig

    def __init__(self):
        self._connection_name: Optional[str] = None
        self._account: Optional[str] = None
        self._database: Optional[str] = None
        self._role: Optional[str] = None
        self._schema: Optional[str] = None
        self._user: Optional[str] = None
        self._password: Optional[str] = None
        self._authenticator: Optional[str] = None
        self._private_key_file: Optional[str] = None
        self._warehouse: Optional[str] = None
        self._mfa_passcode: Optional[str] = None
        self._enable_diag: Optional[bool] = False
        self._diag_log_path: Optional[Path] = None
        self._diag_allowlist_path: Optional[Path] = None
        self._temporary_connection: bool = False
        self._session_token: Optional[str] = None
        self._master_token: Optional[str] = None
        self._token_file_path: Optional[Path] = None

    def clone(self) -> ConnectionContext:
        ctx = ConnectionContext()
        ctx.set_connection_name(self.connection_name)
        ctx.set_account(self.account)
        ctx.set_database(self.database)
        ctx.set_role(self.role)
        ctx.set_schema(self.schema)
        ctx.set_user(self.user)
        ctx.set_password(self.password)
        ctx.set_authenticator(self.authenticator)
        ctx.set_private_key_file(self.private_key_file)
        ctx.set_warehouse(self.warehouse)
        ctx.set_mfa_passcode(self.mfa_passcode)
        ctx.set_enable_diag(self.enable_diag)
        ctx.set_diag_log_path(self.diag_log_path)
        ctx.set_diag_allowlist_path(self.diag_allowlist_path)
        ctx.set_temporary_connection(self.temporary_connection)
        ctx.set_session_token(self.session_token)
        ctx.set_master_token(self.master_token)
        ctx.set_token_file_path(self.token_file_path)
        return ctx

    def update(self, **updates):
        """
        Given a dictionary of property (key, value) mappings, update properties
        of this context object with equivalent names to the keys.

        Raises ValueError if a non-(settable-)property is specified as a key.
        """
        for (key, value) in updates.items():
            # ensure key represents a property
            prop = getattr(type(self), key)
            if not isinstance(prop, property):
                raise ValueError(
                    f"{key} is not a property of {self.__class__.__name__}"
                )

            # our properties don't have setters (fset) but they do follow a convention
            try:
                setter = getattr(self, f"set_{key}")
                setter(value)
            except AttributeError:
                raise ValueError(
                    f"set_{key}() does not exist on {self.__class__.__name__}"
                )

    def __repr__(self):
        items = [
            f"{k} = {repr(v)}" for (k, v) in self.__dict__.items() if v is not None
        ]
        return f"{self.__class__.__name__}({', '.join(items)})"

    @property
    def connection_name(self) -> Optional[str]:
        return self._connection_name

    def set_connection_name(self, value: Optional[str]):
        self._connection_name = value

    @property
    def account(self) -> Optional[str]:
        return self._account

    def set_account(self, value: Optional[str]):
        self._account = value

    @property
    def database(self) -> Optional[str]:
        return self._database

    def set_database(self, value: Optional[str]):
        self._database = value

    @property
    def role(self) -> Optional[str]:
        return self._role

    def set_role(self, value: Optional[str]):
        self._role = value

    @property
    def schema(self) -> Optional[str]:
        return self._schema

    def set_schema(self, value: Optional[str]):
        if (
            value
            and not (value.startswith('"') and value.endswith('"'))
            # if schema is fully qualified name (db.schema)
            and schema_pattern.match(value)
        ):
            raise InvalidSchemaError(value)
        self._schema = value

    @property
    def user(self) -> Optional[str]:
        return self._user

    def set_user(self, value: Optional[str]):
        self._user = value

    @property
    def password(self) -> Optional[str]:
        return self._password

    def set_password(self, value: Optional[str]):
        self._password = value

    @property
    def authenticator(self) -> Optional[str]:
        return self._authenticator

    def set_authenticator(self, value: Optional[str]):
        self._authenticator = value

    @property
    def private_key_file(self) -> Optional[str]:
        return self._private_key_file

    def set_private_key_file(self, value: Optional[str]):
        self._private_key_file = value

    @property
    def warehouse(self) -> Optional[str]:
        return self._warehouse

    def set_warehouse(self, value: Optional[str]):
        self._warehouse = value

    @property
    def mfa_passcode(self) -> Optional[str]:
        return self._mfa_passcode

    def set_mfa_passcode(self, value: Optional[str]):
        self._mfa_passcode = value

    @property
    def enable_diag(self) -> Optional[bool]:
        return self._enable_diag

    def set_enable_diag(self, value: Optional[bool]):
        self._enable_diag = value

    @property
    def diag_log_path(self) -> Optional[Path]:
        return self._diag_log_path

    def set_diag_log_path(self, value: Optional[Path]):
        self._diag_log_path = value

    @property
    def diag_allowlist_path(self) -> Optional[Path]:
        return self._diag_allowlist_path

    def set_diag_allowlist_path(self, value: Optional[Path]):
        self._diag_allowlist_path = value

    @property
    def temporary_connection(self) -> bool:
        return self._temporary_connection

    def set_temporary_connection(self, value: bool):
        self._temporary_connection = value

    @property
    def session_token(self) -> Optional[str]:
        return self._session_token

    def set_session_token(self, value: Optional[str]):
        self._session_token = value

    @property
    def master_token(self) -> Optional[str]:
        return self._master_token

    def set_master_token(self, value: Optional[str]):
        self._master_token = value

    @property
    def token_file_path(self) -> Optional[Path]:
        return self._token_file_path

    def set_token_file_path(self, value: Optional[Path]):
        self._token_file_path = value

    def _collect_not_empty_connection_attributes(self):
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "authenticator": self.authenticator,
            "private_key_file": self.private_key_file,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
            "warehouse": self.warehouse,
            "session_token": self.session_token,
            "master_token": self.master_token,
            "token_file_path": self.token_file_path,
        }

    def build_connection(self):
        from snowflake.cli._app.snow_connector import connect_to_snowflake

        # Ignore warnings about bad owner or permissions on Windows
        # Telemetry omit our warning filter from config.py
        if IS_WINDOWS:
            warnings.filterwarnings(
                action="ignore",
                message="Bad owner or permissions.*",
                module="snowflake.connector.config_manager",
            )

        # ensure we have one of connection_name / temporary_connection
        if not self.temporary_connection and not self.connection_name:
            self._connection_name = get_default_connection_name()

        return connect_to_snowflake(
            temporary_connection=self._temporary_connection,
            mfa_passcode=self._mfa_passcode,
            enable_diag=self._enable_diag,
            diag_log_path=self._diag_log_path,
            diag_allowlist_path=self._diag_allowlist_path,
            connection_name=self._connection_name,
            **self._collect_not_empty_connection_attributes(),
        )


class OpenConnectionCache:
    """
    A connection cache that transparently manages SnowflakeConnection objects
    and is keyed by ConnectionContext objects, e.g. cache[ctx].execute_string(...).
    Connections are automatically closed after CONNECTION_CLEANUP_SEC, but
    are guaranteed to be open (if config is valid) when returned by the cache.
    """

    connections: dict[str, SnowflakeConnection]
    cleanup_futures: dict[str, asyncio.TimerHandle]

    CONNECTION_CLEANUP_SEC: float = 10.0 * 60

    def __init__(self):
        self.connections = {}
        self.cleanup_futures = {}

    def __getitem__(self, ctx):
        if isinstance(ctx, ConnectionContext):
            key = repr(ctx)
            if not self._has_open_connection(key):
                self._insert(key, ctx)
            self._touch(key)
            return self.connections[key]
        else:
            raise ValueError(
                f"Expected key to be ConnectionContext but got {repr(ctx)}"
            )

    def clear(self):
        """Closes all connections and resets the cache to its initial state."""
        for key in self.cleanup_futures:
            self.cleanup_futures[key].cancel()
        self.cleanup_futures.clear()

        for key in self.connections:
            self.connections[key].close()
        self.connections.clear()

    def _has_open_connection(self, key: str):
        return key in self.connections

    def _insert(self, key: str, ctx: ConnectionContext):
        try:
            self.connections[key] = ctx.build_connection()
        except:
            logger.info("ConnectionCache: failed to connect using {key}; not caching.")
            raise

    def _touch(self, key: str):
        """
        Extend the lifetime of the cached connection at the given key.
        """
        if key in self.cleanup_futures:
            self.cleanup_futures.pop(key).cancel()

        loop = asyncio.get_event_loop()
        handle = loop.call_later(
            self.CONNECTION_CLEANUP_SEC, lambda: self._cleanup(key)
        )
        self.cleanup_futures[key] = handle

    def _cleanup(self, key: str):
        """Closes the cached connection at the given key."""
        if key not in self.connections:
            logger.warning("Cleaning up connection {key}, but not found in cache!")

        # doesn't cancel in-flight async queries
        self.connections.pop(key).close()
        del self.cleanup_futures[key]
