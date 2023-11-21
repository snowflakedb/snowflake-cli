from dataclasses import dataclass
from typing import Optional

from snowflake.connector import SnowflakeConnection

from snowcli.output.formats import OutputFormat
from snowcli.snow_connector import connect_to_snowflake

DEFAULT_ENABLE_TRACEBACKS = True
DEFAULT_OUTPUT_FORMAT = OutputFormat.TABLE
DEFAULT_VERBOSE = False
DEFAULT_EXPERIMENTAL = False


@dataclass
class ConnectionContext:
    _cached_connection: Optional[SnowflakeConnection] = None

    connection_name: Optional[str] = None
    account: Optional[str] = None
    database: Optional[str] = None
    role: Optional[str] = None
    schema: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    authenticator: Optional[str] = None
    private_key_path: Optional[str] = None
    warehouse: Optional[str] = None
    temporary_connection: bool = False

    def __setattr__(self, key, value):
        super.__setattr__(self, key, value)
        if key is not "_cached_connection":
            self._cached_connection = None

    @property
    def connection(self) -> SnowflakeConnection:
        if not self._cached_connection:
            self._cached_connection = self._build_connection()
        return self._cached_connection

    def _resolve_connection_params(self):
        from snowcli.cli.common.decorators import GLOBAL_CONNECTION_OPTIONS

        params = {}
        for option in GLOBAL_CONNECTION_OPTIONS:
            override = option.name
            if override == "connection" or override == "temporary_connection":
                continue
            override_value = getattr(self, override)
            if override_value is not None:
                params[override] = override_value
        return params

    def _build_connection(self):
        return connect_to_snowflake(
            temporary_connection=self.temporary_connection,
            connection_name=self.connection_name,
            **self._resolve_connection_params()
        )


class _GlobalContextManager:
    _connection_context = ConnectionContext()

    enable_tracebacks = DEFAULT_ENABLE_TRACEBACKS
    output_format = DEFAULT_OUTPUT_FORMAT
    verbose = DEFAULT_VERBOSE
    experimental = DEFAULT_EXPERIMENTAL

    def reset_context(self):
        self._connection_context = ConnectionContext()
        self.enable_tracebacks = DEFAULT_ENABLE_TRACEBACKS
        self.output_format = DEFAULT_OUTPUT_FORMAT
        self.verbose = DEFAULT_VERBOSE
        self.experimental = DEFAULT_EXPERIMENTAL

    @property
    def connection_context(self) -> ConnectionContext:
        return self._connection_context

    @property
    def connection(self) -> SnowflakeConnection:
        return self.connection_context.connection


class _GlobalContextAccess:
    def __init__(self, manager: _GlobalContextManager):
        self._manager = manager

    @property
    def connection(self) -> SnowflakeConnection:
        return self._manager.connection

    @property
    def enable_tracebacks(self) -> bool:
        return self._manager.enable_tracebacks

    @property
    def output_format(self) -> OutputFormat:
        return self._manager.output_format

    @property
    def verbose(self) -> bool:
        return self._manager.verbose

    @property
    def experimental(self) -> bool:
        return self._manager.experimental


global_context_manager: _GlobalContextManager = _GlobalContextManager()
global_context: _GlobalContextAccess = _GlobalContextAccess(global_context_manager)
