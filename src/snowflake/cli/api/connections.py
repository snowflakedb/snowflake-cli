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
from dataclasses import asdict, dataclass, field, fields, replace
from pathlib import Path
from typing import Optional

from snowflake.cli.api.config import get_default_connection_name
from snowflake.cli.api.exceptions import InvalidSchemaError
from snowflake.connector import SnowflakeConnection
from snowflake.connector.compat import IS_WINDOWS

logger = logging.getLogger(__name__)

schema_pattern = re.compile(r".+\..+")


@dataclass
class ConnectionContext:
    # FIXME: can reduce duplication using config.ConnectionConfig
    connection_name: Optional[str] = None
    account: Optional[str] = None
    database: Optional[str] = None
    role: Optional[str] = None
    schema: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = field(default=None, repr=False)
    authenticator: Optional[str] = None
    private_key_file: Optional[str] = None
    warehouse: Optional[str] = None
    mfa_passcode: Optional[str] = None
    enable_diag: Optional[bool] = False
    diag_log_path: Optional[Path] = None
    diag_allowlist_path: Optional[Path] = None
    temporary_connection: bool = False
    session_token: Optional[str] = None
    master_token: Optional[str] = None
    token_file_path: Optional[Path] = None

    VALIDATED_FIELD_NAMES = ["schema"]

    def clone(self) -> ConnectionContext:
        return replace(self)

    def update(self, **updates):
        """
        Given a dictionary of property (key, value) mappings, update properties
        of this context object with equivalent names to the keys.

        Raises KeyError if a non-property is specified as a key.
        """
        field_map = {field.name: field for field in fields(self)}
        for (key, value) in updates.items():
            # ensure key represents a property
            if key not in field_map:
                raise KeyError(f"{key} is not a field of {self.__class__.__name__}")
            setattr(self, key, value)

    def __repr__(self) -> str:
        """Minimal repr where empty (i.e. None) values have their keys omitted."""
        items = [
            f"{k}={repr(v)}"
            for (k, v) in self.as_nonempty_dict().items()
            if v is not None
        ]
        return f"{self.__class__.__name__}({', '.join(items)})"

    def __setattr__(self, prop, val):
        """Runs registered validators before setting fields."""
        if prop in self.VALIDATED_FIELD_NAMES:
            validate = getattr(self, f"validate_{prop}")
            validate(val)
        super().__setattr__(prop, val)

    def validate_schema(self, value: Optional[str]):
        if (
            value
            and not (value.startswith('"') and value.endswith('"'))
            # if schema is fully qualified name (db.schema)
            and schema_pattern.match(value)
        ):
            raise InvalidSchemaError(value)

    def validate_and_complete(self):
        """
        Ensure we can create a connection from this context.
        """
        if not self.temporary_connection and not self.connection_name:
            self.connection_name = get_default_connection_name()

    def as_nonempty_dict(self) -> dict:
        return {k: v for (k, v) in asdict(self).items() if v is not None}

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

        return connect_to_snowflake(**self.as_nonempty_dict())


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
    """Connections are closed this many seconds after the last time they are accessed."""

    def __init__(self):
        self.connections = {}
        self.cleanup_futures = {}

    def __getitem__(self, ctx):
        if not isinstance(ctx, ConnectionContext):
            raise ValueError(
                f"Expected key to be ConnectionContext but got {repr(ctx)}"
            )
        key = repr(ctx)
        if not self._has_open_connection(key):
            self._insert(key, ctx)
        self._touch(key)
        return self.connections[key]

    def clear(self):
        """Closes all connections and resets the cache to its initial state."""
        connection_keys = list(self.connections.keys())
        for key in connection_keys:
            self._cleanup(key)

        # if any orphaned futures still exist, clean them up too
        for key in self.cleanup_futures:
            self.cleanup_futures[key].cancel()
        self.cleanup_futures.clear()

    def _has_open_connection(self, key: str):
        return key in self.connections

    def _insert(self, key: str, ctx: ConnectionContext):
        try:
            self.connections[key] = ctx.build_connection()
        except Exception:
            logger.debug("ConnectionCache: failed to connect using {key}; not caching.")
            raise

    def _cancel_cleanup_future_if_exists(self, key: str):
        if key in self.cleanup_futures:
            self.cleanup_futures.pop(key).cancel()

    def _touch(self, key: str):
        """
        Extend the lifetime of the cached connection at the given key.
        """
        try:
            loop = asyncio.get_event_loop()
        except (RuntimeError, DeprecationWarning):
            # the exception is different for Python 3.10/3.11+
            logger.debug(
                "ConnectionCache: no event loop; connections will close at exit."
            )
            return

        handle = loop.call_later(
            self.CONNECTION_CLEANUP_SEC, lambda: self._cleanup(key)
        )
        self._cancel_cleanup_future_if_exists(key)
        self.cleanup_futures[key] = handle

    def _cleanup(self, key: str):
        """Closes the cached connection at the given key."""
        if key not in self.connections:
            logger.debug("Cleaning up connection {key}, but not found in cache!")

        # doesn't cancel in-flight async queries
        self._cancel_cleanup_future_if_exists(key)
        self.connections.pop(key).close()
