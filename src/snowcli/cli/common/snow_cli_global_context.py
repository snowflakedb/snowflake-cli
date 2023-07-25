from copy import deepcopy
from dataclasses import dataclass
from typing import Callable, Optional

from snowcli.config import cli_config, get_default_connection
from snowcli.snow_connector import connect_to_snowflake


@dataclass
class ConnectionDetails:
    conn_name: str

    account: Optional[str] = None
    database: Optional[str] = None
    role: Optional[str] = None
    schema: Optional[str] = None
    user: Optional[str] = None
    warehouse: Optional[str] = None

    def connection_params(self):
        params = cli_config.get_connection(self.conn_name)

        overrides = ["account", "database", "role", "schema", "user", "warehouse"]
        for override in overrides:
            override_value = getattr(self, override)
            if override_value is not None:
                params[override] = override_value
        return params

    @staticmethod
    def _connection_update(param_name: str, value: str):
        def modifications(context: SnowCliGlobalContext) -> SnowCliGlobalContext:
            setattr(context.connection, param_name, value)
            return context

        snow_cli_global_context_manager.update_global_context(modifications)
        return value

    @staticmethod
    def update_callback(param_name: str):
        return lambda value: ConnectionDetails._connection_update(
            param_name=param_name, value=value
        )


@dataclass
class SnowCliGlobalContext:
    """
    Global state accessible in whole CLI code.
    """

    enable_tracebacks: bool
    connection: ConnectionDetails


class SnowCliGlobalContextManager:
    """
    A manager responsible for retrieving and updating global state.
    """

    def __init__(self, global_context_with_default_values: SnowCliGlobalContext):
        self._global_context = deepcopy(global_context_with_default_values)

    def get_global_context_copy(self) -> SnowCliGlobalContext:
        """
        Returns deep copy of global state.
        """
        return deepcopy(self._global_context)

    def update_global_context(
        self, update: Callable[[SnowCliGlobalContext], SnowCliGlobalContext]
    ) -> None:
        """
        Updates global state using provided function.
        The resulting object will be deep copied before storing in the manager.
        """
        self._global_context = deepcopy(update(self.get_global_context_copy()))

    def get_connection(self):
        connection = self.get_global_context_copy().connection
        return connect_to_snowflake(
            connection_name=connection.conn_name, **connection.connection_params()
        )


def _create_snow_cli_global_context_manager_with_default_values() -> SnowCliGlobalContextManager:
    """
    Creates a manager with global state filled with default values.
    """
    return SnowCliGlobalContextManager(
        SnowCliGlobalContext(
            enable_tracebacks=True,
            connection=ConnectionDetails(conn_name=get_default_connection()),
        )
    )


snow_cli_global_context_manager = (
    _create_snow_cli_global_context_manager_with_default_values()
)
