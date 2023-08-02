from copy import deepcopy
from dataclasses import dataclass
from typing import Callable, Optional

from snowcli.config import cli_config, get_default_connection
from snowcli.snow_connector import connect_to_snowflake


@dataclass
class ConnectionDetails:
    connection_name: Optional[str] = None
    account: Optional[str] = None
    database: Optional[str] = None
    role: Optional[str] = None
    schema: Optional[str] = None
    user: Optional[str] = None
    warehouse: Optional[str] = None

    def _resolve_connection_params(self):
        from snowcli.cli.common.decorators import GLOBAL_CONNECTION_OPTIONS

        params = {}
        for option in GLOBAL_CONNECTION_OPTIONS:
            override = option.name
            if override == "connection":
                continue
            override_value = getattr(self, override)
            if override_value is not None:
                params[override] = override_value
        return params

    def build_connection(self):
        return connect_to_snowflake(
            connection_name=self.connection_name, **self._resolve_connection_params()
        )

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
        return self.get_global_context_copy().connection.build_connection()


def _create_snow_cli_global_context_manager_with_default_values() -> SnowCliGlobalContextManager:
    """
    Creates a manager with global state filled with default values.
    """
    return SnowCliGlobalContextManager(
        SnowCliGlobalContext(
            enable_tracebacks=True,
            connection=ConnectionDetails(),
        )
    )


snow_cli_global_context_manager = (
    _create_snow_cli_global_context_manager_with_default_values()
)
