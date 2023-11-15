from dataclasses import dataclass
from typing import Optional, Union, Callable, Any

from snowflake.connector import SnowflakeConnection

from snowcli.output.formats import OutputFormat
from snowcli.snow_connector import connect_to_snowflake


@dataclass
class ConnectionDetails:
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

    def build_connection(self):
        return connect_to_snowflake(
            temporary_connection=self.temporary_connection,
            connection_name=self.connection_name,
            **self._resolve_connection_params()
        )


DEFAULT_ENABLE_TRACEBACKS = True
DEFAULT_OUTPUT_FORMAT = OutputFormat.TABLE
DEFAULT_VERBOSE = False
DEFAULT_EXPERIMENTAL = False


class _GlobalContextManager:
    _connection_details = ConnectionDetails()
    _cached_connection: Optional[SnowflakeConnection] = None

    enable_tracebacks = DEFAULT_ENABLE_TRACEBACKS
    output_format = DEFAULT_OUTPUT_FORMAT
    verbose = DEFAULT_VERBOSE
    experimental = DEFAULT_EXPERIMENTAL

    def reset_context(self):
        self.connection_details = ConnectionDetails()
        self.enable_tracebacks = DEFAULT_ENABLE_TRACEBACKS
        self.output_format = DEFAULT_OUTPUT_FORMAT
        self.verbose = DEFAULT_VERBOSE
        self.experimental = DEFAULT_EXPERIMENTAL

    @property
    def connection_details(self) -> ConnectionDetails:
        return self._connection_details

    @connection_details.setter
    def connection_details(self, connection_details: ConnectionDetails):
        self._connection_details = connection_details
        self._cached_connection = None

    @property
    def connection(self) -> SnowflakeConnection:
        if not self._cached_connection:
            self._cached_connection = self.connection_details.build_connection()
        return self._cached_connection

    def update_global_context_option(self, param_name: str, value: Union[bool, str]):
        setattr(self, param_name, value)

    def update_global_connection_detail(self, param_name: str, value: str):
        setattr(self._connection_details, param_name, value)
        self._cached_connection = None


class _GlobalContextAccess:
    def __init__(self, manager: _GlobalContextManager):
        self._manager = manager

    @property
    def connection_details(self) -> ConnectionDetails:
        return self._manager.connection_details

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


def _update_callback(update: Callable[[Any], Any]):
    def callback(value):
        update(value)
        return value

    return callback


def update_global_option_callback(param_name: str):
    return _update_callback(
        lambda value: (
            global_context_manager.update_global_context_option(
                param_name=param_name, value=value
            )
        )
    )


def update_global_connection_detail_callback(param_name: str):
    return _update_callback(
        lambda value: global_context_manager.update_global_connection_detail(
            param_name=param_name, value=value
        )
    )
